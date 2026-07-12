# Taskflow

Taskflow est un framework Go léger permettant de définir et d'exécuter des tâches métier sous forme de machines à états
finis (FSM).

L'objectif n'est pas de fournir un moteur de workflow complet avec ordonnanceur, interface graphique ou moteur de 
règles, mais de proposer une base simple et robuste pour exécuter des processus métier composés d'étapes successives.

## Principes

Taskflow repose sur quelques idées simples :

* un processus métier est décrit comme une machine à états finis ;
* chaque état correspond à une étape d'exécution ;
* chaque étape possède un handler métier ;
* le résultat d'une étape produit un événement ;
* l'événement détermine la transition vers l'état suivant ;
* l'exécution peut être reprise à partir du dernier état connu.

Le framework sépare clairement :

```
workflow
    |
    | définit le processus métier
    |
runtime
    |
    | exécute une instance de tâche
    |
storage
    |
    | persiste l'avancement
```

## Exemple simple

Définition d'un workflow :

```go
builder := workflow.New[ImportData]("import")

builder.
    State("Download", downloadHandler).
    Success("Validate").
    Failure("Failed")

builder.
    State("Validate", validateHandler).
    Success("Store").
    Failure("Failed")

builder.
    State("Store", storeHandler)

builder.
    State("Failed", failedHandler)

wf, err := builder.Build()

if err != nil {
    panic(err)
}
```

Le workflow obtenu est une définition compilée et immutable.

## Etats et transitions

Un état représente une étape métier.

Exemple :

```go
func downloadHandler(
    ctx context.Context,
    data *ImportData,
) (workflow.Event, error) {

    // traitement métier

    return workflow.Success, nil
}
```

Les événements disponibles sont :

* `Success`
* `Failure`
* `Retry`
* `Cancel`
* `Timeout`

Une transition est définie par :

```
état courant + événement = état suivant
```

Exemple :

```
Download
    |
    | Success
    v
Validate
```

## Validation

Lors de l'appel à `Build()`, Taskflow vérifie notamment :

* les états dupliqués ;
* les transitions dupliquées ;
* les transitions vers des états inexistants ;
* l'existence d'un état initial ;
* les états inaccessibles.

Un workflow invalide ne peut pas être exécuté.

## Architecture actuelle

Le package `workflow` contient :

```
workflow/
├── builder.go
├── compile.go
├── errors.go
├── event.go
├── handler.go
├── transition.go
├── workflow.go
└── workflow_test.go
```

### Builder

Le builder sert uniquement à déclarer un workflow.

Il conserve :

* les états ;
* les handlers ;
* les transitions.

### Compile

La compilation transforme la définition déclarative en une structure optimisée pour l'exécution :

```
Builder

   Build()

Workflow
```

Le workflow compilé utilise une représentation rapide :

```go
map[state]map[event]nextState
```

permettant une résolution de transition en temps constant.

## Tests

Le package workflow possède des tests couvrant :

* la construction d'un workflow ;
* les transitions valides ;
* les états inconnus ;
* les transitions dupliquées ;
* les états inaccessibles.

Lancer les tests :

```bash
go test ./...
```

## Roadmap

### Runtime

Le prochain composant est le runtime d'exécution.

Il aura pour rôle :

* charger une instance de tâche ;
* exécuter le handler de l'état courant ;
* récupérer l'événement produit ;
* appliquer la transition ;
* sauvegarder la progression.

### Storage

Une couche de persistance permettra ensuite de stocker :

* l'état courant ;
* les données métier ;
* l'historique des transitions ;
* les erreurs d'exécution.

Une implémentation PostgreSQL est prévue.

### Worker

Les workers permettront une exécution distribuée :

* un worker récupère une tâche disponible ;
* il exécute une ou plusieurs étapes ;
* il persiste l'avancement ;
* un autre worker peut reprendre après interruption.

## Philosophie

Taskflow privilégie :

* la simplicité ;
* la transparence ;
* la persistance explicite ;
* la robustesse face aux interruptions ;
* une séparation claire entre définition métier et exécution.

Le framework ne cherche pas à remplacer les moteurs BPM complexes, mais à fournir une solution légère et idiomatique 
pour des processus métier contrôlés par du code Go.
