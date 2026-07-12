# Taskflow

Taskflow est un framework Go permettant de définir et d'exécuter des processus métier sous forme de 
**machines à états finis** (Finite State Machines - FSM).

Il fournit les briques nécessaires pour construire des traitements robustes, persistants et distribués,
tout en laissant le code métier au cœur de l'application.

Taskflow ne cherche pas à remplacer un moteur BPM complet. Il privilégie une approche simple, 
idiomatique et entièrement pilotée par le code Go.

## Caractéristiques

* définition déclarative des workflows ;
* validation du workflow à la construction ;
* exécution étape par étape ;
* reprise après interruption ;
* persistance indépendante de l'implémentation ;
* contrôle de concurrence optimiste ;
* acquisition concurrente des tâches ;
* exécution distribuée grâce à plusieurs workers.

## Architecture

Taskflow est organisé en plusieurs packages indépendants :

```
workflow
    │
    ▼
runtime
    │
    ▼
worker
    │
    ▼
executor
    ▲
    │
storage
```

Chaque package possède une responsabilité unique.

| Package    | Description                                   |
| ---------- | --------------------------------------------- |
| `workflow` | Définition et validation d'un workflow.       |
| `runtime`  | Exécution d'une étape d'une tâche.            |
| `storage`  | Contrats de persistance et erreurs communes.  |
| `memory`   | Implémentation de stockage en mémoire.        |
| `pgsql`    | Implémentation PostgreSQL.                    |
| `worker`   | Exécute une tâche jusqu'à son terme.          |
| `executor` | Acquiert les tâches et pilote leur exécution. |

## Définir un workflow

Un workflow est construit à l'aide du builder.

```go
builder := workflow.New[ImportData]("import")

builder.
	State("download", downloadHandler).
	Success("validate").
	Failure("failed")

builder.
	State("validate", validateHandler).
	Success("store").
	Failure("failed")

builder.
	State("store", storeHandler).
	Complete()

builder.
	State("failed", failedHandler).
	Complete()

builder.Initial("download")

wf, err := builder.Build()
if err != nil {
	panic(err)
}
```

Une fois construit, un workflow est immutable et peut être partagé entre plusieurs goroutines.

## Les handlers

Chaque état est associé à un handler métier.

```go
func downloadHandler(
	ctx context.Context,
	data *ImportData,
) (workflow.Event, error) {

	// traitement métier

	return workflow.Success, nil
}
```

Le handler :

* reçoit le contexte d'exécution ;
* modifie les données métier ;
* retourne un événement ;
* peut retourner une erreur.

Un panic est automatiquement converti en erreur par le runtime.

## Les transitions

Les transitions sont déclenchées par les événements retournés par les handlers.

Les événements prédéfinis sont :

* `Success`
* `Failure`
* `Retry`
* `Cancel`
* `Timeout`

Une transition est définie par :

```
état courant
        +
événement
        =
état suivant
```

## Validation

Lors de l'appel à `Build()`, Taskflow vérifie notamment :

* les états dupliqués ;
* les transitions dupliquées ;
* les transitions invalides ;
* l'existence de l'état initial ;
* les états inaccessibles.

Un workflow invalide ne peut pas être exécuté.

## Exécution

Le package `runtime` exécute une étape du workflow.

```go
runner := runtime.NewRunner(wf)

task := runner.NewTask(
	data,
	runtime.DefaultQueue,
)

result, err := runner.Step(ctx, task)
```

Chaque appel à `Step()` :

* exécute le handler de l'état courant ;
* applique la transition correspondante ;
* met à jour l'état de la tâche.

## Persistance

Taskflow sépare complètement l'exécution de la persistance.

Le package `storage` définit les contrats utilisés par les autres composants.

Deux implémentations sont actuellement disponibles :

* `memory`, adaptée aux tests ou aux applications simples ;
* `pgsql`, basée sur PostgreSQL.

L'implémentation PostgreSQL utilise notamment :

* le verrouillage optimiste via un numéro de version ;
* `FOR UPDATE SKIP LOCKED` pour permettre à plusieurs executors de consommer la même file de tâches sans conflit.

## Worker

Le package `worker` exécute une tâche jusqu'à son terme.

À chaque étape, il :

* exécute le handler métier ;
* met à jour le statut de la tâche ;
* persiste sa progression.

En cas d'erreur métier, la tâche est marquée comme échouée avant que l'erreur ne soit renvoyée.

## Executor

Le package `executor` pilote l'exécution continue des tâches.

Il :

* acquiert une tâche disponible ;
* la confie au worker ;
* applique un backoff lorsqu'aucune tâche n'est disponible ;
* peut continuer ou s'arrêter selon la politique configurée lorsqu'une erreur survient.

Plusieurs executors peuvent fonctionner simultanément sur la même base PostgreSQL afin de répartir automatiquement
la charge.

## Exemple

```go
runner := runtime.NewRunner(workflow)

repo, err := pgsql.Open[Data](dsn)
if err != nil {
	panic(err)
}
defer repo.Close()

worker := worker.New(
	runner,
	repo,
)

consumer, err := executor.NewConsumer(
	repo,
	worker,
)
if err != nil {
	panic(err)
}

if err := consumer.Serve(ctx); err != nil {
	log.Fatal(err)
}
```

## Tests

L'ensemble des packages est couvert par des tests unitaires.

Exécuter tous les tests :

```bash
go test ./...
```

## Philosophie

Taskflow privilégie :

* la simplicité ;
* la lisibilité ;
* la robustesse ;
* la persistance explicite ;
* la reprise après interruption ;
* la séparation entre logique métier et infrastructure.

L'objectif est de fournir un framework léger et idiomatique permettant de construire des traitements métier fiables,
tout en laissant aux applications le contrôle de leur architecture.
