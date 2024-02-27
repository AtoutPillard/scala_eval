

## 6. Conclusion et évaluation 

### a) Conclusion

Avec ce cours, nous avons compris l'**architecture distribuée** employée par Spark avec notamment le rôle du `driver` et des `executors`. À l'aide de cette architecture, nous pouvons traiter de gros volumes de données plus efficacement qu'avec `Pandas` par exemple.  

Nous avons utilisé Spark de deux manières. La première avec le `spark-shell` qui nous permettait de se familiariser avec les différentes fonctions de Spark de et la seconde depuis un fichier Scala et l'outil `sbt`

Nous avons la possibilité de traiter **différents types de données (CSV,JSON, données textuelles)** mais il est possible d'utiliser des types de données plus utilisés en **Big Data (ORC,Parquet,Avro)** qui sont plus performants dans ce contexte. 

Pour traiter ces données, nous avons utilisé différentes structures que nous résumons ici : 

- `RDD`

Il s'agit de la structure de base disponible sur Spark pour faire du **calcul parallélisable**. Nous transformons une collection d'éléments en un `RDD` qui lui est segmenté en plusieurs **partitions**. Ce sont ces partitions qui permettent de faire du calcul distribué. Il s'agit d'une **structure bas niveau**, son utilisation est assez rare car nous devons configurer plus de paramètres et que certaines opérations ne sont pas optimisées. 

- `DataFrame` 

Une structure tabulaire et qui se rapproche des `DataFrame` de `Pandas`, nous y avons vu différentes actions (modifier les colonnes, filtrer les lignes, grouper des modalités, joindre des tables). 
De plus, à l'aide de cette structure, il est possible de faire des **opérations `SQL`**, facilitant grandement les opérations sur les données importées de différents types. Contrairement aux `RDD`, ici les opérations sont optimisées avec les `DataFrame` donc il faudra les .

- `Dataset` 

Spécificité des langages typés comme **Java** ou **Scala**. Dans les faits, son usage est très proche d'un `DataFrame` et même nous avons l'équivalence `Dataset[Row]` ~ `DataFrame`. Son principal atout est d'apporter plus de sûreté à notre application Spark comme ce que ferait un **langage compilé**.

Enfin, nous avons vu quelques aspects de **l'interface web** et comment **améliorer les performances** de notre application Spark.

D'autres librairies sont disponibles sur Spark, pour faire du **Machine Learning**, il suffira de charger la librairie MLlib (son utilisation est proche de celle abordée dans le cours sur PySpark) mais aussi de traiter les données en streaming qui seront abordées au prochain sprint.

### b) Évaluation

Pour consolider vos connaissances sur Scala et Spark, vous allez réaliser l'exercice suivant. Le but sera de faire un simple **ETL** à l'aide de cette [API](https://www.balldontlie.io/#introduction) donnant des données sur la NBA avec Scala, puis de traiter les données collectées avec Spark pour avoir des fichiers `csv`. Il n'y a pas besoin de clé pour pouvoir requêter cette API. 

Nous avons 2 grandes étapes : l'**extraction** et la **transformation**. Nous allons récupérer des données sur les matchs des équipes suivantes : `Phoenix Suns`, `Atlanta Hawks`, `Los Angeles Lakers` et `Milwaukee Bucks`. Puis, nous allons récupérer les statistiques des joueurs de ces matchs. Pour finir, nous allons joindre les deux datasets ensemble à l'aide de Spark.

#### *Création d'une clé*

Pour requêter l'API, il est nécessaire de créer une clé.

> Rendez-vous sur le site de l'[API](https://www.balldontlie.io/#introduction).  
> Puis, cliquez sur `our website` pour vous redirigez vers la page de connexion.  

<p align="center">
    <img src="https://dst-de.s3.eu-west-3.amazonaws.com/scala_spark_fr/balldontlie_apikey_1.png" style="width:62%">
</p>

> Créez un compte en cliquant sur `SIGN UP`.

<p align="center">
    <img src="https://dst-de.s3.eu-west-3.amazonaws.com/scala_spark_fr/balldontlie_apikey_2.png" style="width:62%">
</p>

Une fois le compte créé, une page similaire à celle-ci devrait apparaître.

<p align="center">
    <img src="https://dst-de.s3.eu-west-3.amazonaws.com/scala_spark_fr/balldontlie_apikey_3.png" style="width:62%">
</p>

<div class="alert alert-info"><i class="icon circle info"></i>
Sauvegardez votre clé dans un fichier !
</div>


#### *Prise en main de l'API et de la librairie ujson*

Afin de vous familiariser avec l'API, suivez cet exercice.

Dans cette partie, nous allons extraire les ids de nos équipes depuis la route [`teams`](https://www.balldontlie.io/#teams).

Commençons par construire notre fichier `build.sbt`.  
> Donnez le nom que vous souhaitez à votre projet et sa version.  
> Utilisez la version `2.12.15` de Scala.  
> Ajoutez les librairies `requests` et `ujson`.

%%SOLUTION%%

```scala
name := "NBA"

version := "1.0"

scalaVersion := "2.12.15"

libraryDependencies += "com.lihaoyi" %% "requests" % "0.1.8"
libraryDependencies += "com.lihaoyi" %% "ujson" % "0.7.1"
```

%%SOLUTION%%

Il n'est pas nécessaire d'importer les librairies `requests` et `ujson` dans nos fichiers pour les utiliser.

Nous allons créer notre objet `Extract` dans lequel nous instancerions 4 variables globales `teams_url`, `team_names`, `api_key` et `headers`. Puis, nous créerons 4 fonctions :

- `extract_page` qui extrait le contenu json à l'aide d'une requête GET ;
- `extract_pages_data` qui itère sur les pages renvoyées par la route d'une API et extrait les données de celles-ci ;
- `extract_team_ids` qui extrait les ids des équipes sélectionnées ;
- `main` afin de lancer notre programme.

> Créez le fichier `Extract.scala` et instanciez un objet `Extract`.  
> Instanciez `teams_url` qui est la chaîne de caractères de notre route.  
> Instanciez une liste de String nommé `team_names` avec le nom des équipes suivantes : `Phoenix Suns`, `Atlanta Hawks`, `Los Angeles Lakers` et `Milwaukee Bucks`.  
> Instanciez la chaîne de caractères `api_key` qui représente votre clé pour requêter l'API.  
> Instanciez un objet de type Map[String, String] `headers` et associez `api_key` à la chaine de caractères `Authorization`.

%%SOLUTION%%

```scala
object Extract {
  val teams_url: String = "https://api.balldontlie.io/v1/teams"
  val team_names: List[String] = List[String]("Phoenix Suns", "Atlanta Hawks", "Los Angeles Lakers", "Milwaukee Bucks")
  val api_key: String = "<copier-coller votre clé>"
  val headers: Map[String, String] = Map("Authorization" -> api_key)
}
```

%%SOLUTION%%

- `extract_page`

> Créez une fonction `extract_page` qui prend en argument une chaîne de caractères correspondant à une route et qui renvoie un `ujson.Value.Value`.  
> Utilisez la fonction `requests.get` pour extraire le retour d'une requête GET. Pensez à rajouter votre variable `headers` à la fonction `requests.get`.  
> Utiliser la fonction `ujson.read` pour transformer le contenu json en `ujson.Value.Value`.

%%SOLUTION%%

```scala
def extract_page(url: String): ujson.Value.Value = {
  /*
  Extract json content of a page using a http get request
  Arg:
    url: String -> the url we extract the json content from
  Return:
    ujs: usjon.Value.Value -> json content of the page
  */
  val res: requests.Response = requests.get(url, headers = headers)
  val ujs: ujson.Value.Value = ujson.read(res.text)
  return ujs
}
```

%%SOLUTION%%

Prenez le temps d'observer le contenu json retourné.

- `extract_pages_data`

> Construisez la fonction `extract_pages_data` qui prend en argument une chaîne de caractères correspondant à une route et qui renvoie une `List[ujson.Value]`.  
> Instanciez dans une variable `pages_data` une `List[ujson.Value]` vide.  

<div class="alert alert-warning"> <i class="fa fa-info-circle" style="margin: 5px"></i>
Comme nous allons itérer sur nos pages, nous allons créer une boucle <B>while</B> qui se terminera lorsque la valeur de l'attribut <B>metadata.next_cursor</B> vaut <B>ujson.Null</B>.
</div>

> Instanciez une variable `cursor` de type `ujson.Value` avec la valeur 1.  
> Construisez une boucle `while` qui se termine lorsque `cursor` vaut `ujson.Null`.  

Dans le **while** :  
> Dans une variable `ujs` de type `ujson.Value.Value`, récupérez le contenu de l'url en paramètre de la fonction et rajoutez à celle-ci le paramètre `cursor`. Ce dernier aura la valeur de la variable `cursor`.  
> Dans une variable `data` de type `ujson.Value`, récupérez depuis `ujs` les donnnées de l'attribut `data`.  
> Ajoutez le contenu de la variable `data` à la liste `pages_data`.  

L'API renvoie les données dans une liste où se trouve 2 dictionnaires `data` et `metadata`. Cependant, parfois le dictionnaire `metadata` n'est pas présent. Nous allons utiliser l'instruction **try...catch** afin de ne pas bloquer notre code en cas d'erreur.

- Dans le **try** :  
> Dans une variable `metadata` de type `ujson.Value`, récupérez depuis `ujs` les données de l'attribut `metadata`.  
> Remplacez la valeur de la variable `cursor` avec celle contenu dans la variable `metadata`.  

- Dans le **catch** :  
> À l'aide le l'instruction `case`, récupérez l'exception dans le cas où celle-ci est de type `Exception` et affichez un message d'erreur.  

> Renvoyez la variable `pages_data`.

%%SOLUTION%%

```scala
def extract_pages_data(url: String): List[ujson.Value] = {
  /*
  Iterate through the pages of an api route result
  Select and extract the data from each page
  Arg:
    url: String -> the url we iterate through
  Return:
    pages_data: List[ujson.Value] -> data of each page
  */
  var pages_data: List[ujson.Value] = List[ujson.Value]()
  var cursor: ujson.Value = 1
  while (cursor != ujson.Null) {
    val ujs: ujson.Value.Value = extract_page(s"${url}?cursor=${cursor}")
    val data: ujson.Value = ujs("data")
    pages_data :+= data
    try {
      val metadata: ujson.Value = ujs("meta")
      cursor = metadata("next_cursor")
    } catch {
      case e: Exception =>
        println(s"There isn't any page left for ${url}")
        cursor = ujson.Null
    }
  }
  return pages_data
}
```

%%SOLUTION%%

- `extract_team_ids`

> Créez la fonction `extract_team_ids` qui prend en argument 2 variables: une chaîne de caractères correspondant à une route et la liste du nom de équipes. La fonction renvoie une `List[Int]`.  
> Créez une variable `pages_data` qui récupère le contenu du résultat de la fonction `extract_pages_data`.  
> Instanciez une `List[Int]` vide dans une variable nommée `team_ids`.  

Nous allons créer une boucle `for` afin d'itérer sur les pages. Dans celle-ci, nous filtrerons les équipes de la page et ne garderons que les équipes contenues dans la liste `team_names`. Ensuite, nous ajouterons les ids de ces équipes dans notre liste `team_ids`. 

> Faites une boucle `for` qui itère sur `pages_data` et renvoie donc `page_data`.  
> Filtrez les équipes selon leur attribut *full_name* et enregistrez le résultat dans une variable `teams_selected`. Nous devons ajouter la méthode `arr` afin de créer un objet `scala.collection.mutable.ArrayBuffer` sur lequel nous pouvons utiliser la fonction `filter`. 
> Extrayez l'id de chaque équipe de la variable `teams_selected` et ajoutez-les à notre liste `team_ids`. Vous pouvez utiliser `arr` et `foreach` pour tâche. Pour transformer l'id en Int, nous devons utiliser la méthode `num` puis `toInt`. Malheureusement, la classe `ujson.Value` ne possède pas la méthode `toInt`, c'est pourquoi nous devons transformer notre id en double à l'aide de `num` avant.  
> Sortez de la boucle et renvoyez la liste `team_ids`.

%%SOLUTION%%

```scala
def extract_team_ids(
  teams_url: String,
  team_names: List[String]
): List[Int] = {
  /*
  Extract the selected team ids
  Args:
    teams_url: String -> the api route giving team informations
    team_names: List[String] -> the team names
  Return:
    team_ids: List[Int] -> the team ids of the selected teams
  */
  val pages_data = extract_pages_data(teams_url)
  var team_ids: List[Int] = List[Int]()
  for (page_data <- pages_data) {
    val teams_selected = page_data.arr.filter(
      team => team_names.contains(team("full_name").toString.replace("\"", "")))
    teams_selected.arr.foreach(team => team_ids :+= team("id").num.toInt)
  }
  return team_ids
}
```

%%SOLUTION%%

> Ajoutez la fonction `main` pour tester vos fonctions.

Voici le code final :

%%SOLUTION%%

```scala
object Extract {
  val teams_url: String = "https://api.balldontlie.io/v1/teams"
  val team_names: List[String] = List[String]("Phoenix Suns", "Atlanta Hawks", "Los Angeles Lakers", "Milwaukee Bucks")
  val api_key: String = "<copier-coller votre clé>"
  val headers: Map[String, String] = Map("Authorization" -> api_key)

  def extract_page(url: String): ujson.Value.Value = {
    /*
    Extract json content of a page using a http get request
    Arg:
      url: String -> the url we extract the json content from
    Return:
      ujs: usjon.Value.Value -> json content of the page
    */
    val res: requests.Response = requests.get(url, headers = headers)
    val ujs: ujson.Value.Value = ujson.read(res.text)
    return ujs
  }

  def extract_pages_data(url: String): List[ujson.Value] = {
    /*
    Iterate through the pages of an api route result
    Select and extract the data from each page
    Arg:
      url: String -> the url we iterate through
    Return:
      pages_data: List[ujson.Value] -> data of each page
    */
    var pages_data: List[ujson.Value] = List[ujson.Value]()
    var cursor: ujson.Value = 1
    while (cursor != ujson.Null) {
      val ujs: ujson.Value.Value = extract_page(s"${url}?cursor=${cursor}")
      val data: ujson.Value = ujs("data")
      pages_data :+= data
      try {
        val metadata: ujson.Value = ujs("meta")
        cursor = metadata("next_cursor")
      } catch {
        case e: Exception =>
          println(s"There isn't any page left for ${url}")
          cursor = ujson.Null
      }
    }
    return pages_data
  }

  def extract_team_ids(
    teams_url: String,
    team_names: List[String]
  ): List[Int] = {
    /*
    Extract the selected team ids
    Args:
      teams_url: String -> the api route giving team informations
      team_names: List[String] -> the team names
    Return:
      team_ids: List[Int] -> the team ids of the selected teams
    */
    val pages_data = extract_pages_data(teams_url)
    var team_ids: List[Int] = List[Int]()
    for (page_data <- pages_data) {
      val teams_selected = page_data.arr.filter(
        team => team_names.contains(team("full_name").toString.replace("\"", "")))
      teams_selected.arr.foreach(team => team_ids :+= team("id").num.toInt)
    }
    return team_ids
  }

  def main(args: Array[String]) {
    /*
    Extract 4 team ids from the balldontlie api
    */
    team_names.foreach(println)
    val team_ids = extract_team_ids(teams_url, team_names)
    team_ids.foreach(println)
  }
}
```

<div class="alert alert-warning"> <i class="fa fa-info-circle" style="margin: 5px"></i>
Les APIs imposent, très souvent, des limites sur le nombre d'appels. Celle-ci limite le nombre de requête à 30 par minutes. Pensez à utiliser l'instruction <B>try...catch</B> afin de ne pas arrêter notre programme si un appel à l'API échoue.
</div>

%%SOLUTION%%

Passons maintenant à l'évaluation en autonomie.

#### *Architecture du projet*

Créez un fichier build.sbt avec les dépendances suivantes :

- `requests` : pour requêter l'API
- `ujson` : pour lire le contenu de la réponse de l'API
- `os-lib` : pour créer vos dossiers et y écrire des fichiers json 
- `spark-sql` : pour instancier une session spark
    
Créez les dossiers suivants :

- `src/main/scala/` qui contiendra vos programmes en scala
- `games/` qui contiendra vos fichiers json sur les matchs
- `stats/` qui contiendra vos fichiers json sur les stats suivant les matchs
- `csv/` qui contiendra vos fichiers csv final

Faites des fonctions !

Utilisez des chemins RELATIFS pour gérer vos fichiers !

#### *Extraction des données* 

Pour requêter des API `HTTP`, nous passons principalement par la librairie `requests` de Python. Il existe un [équivalent](https://github.com/com-lihaoyi/requests-scala) sur Scala, comportant les principales fonctionnalités que celle sur Python. 

L'[API](https://www.balldontlie.io/#introduction) comporte les routes suivantes : 

- [`games`](https://www.balldontlie.io/#get-all-games)

- [`stats`](https://www.balldontlie.io/#get-all-stats)

> Collectez les matchs de la dernière saison des équipes suivantes : 

- `Phoenix Suns`

- `Atlanta Hawks`

- `Los Angeles Lakers`

- `Milwaukee Bucks`

> Collectez ensuite les statistiques des matchs sélectionnées en amont. 

<div class="alert alert-warning"> <i class="fa fa-info-circle" style="margin: 5px"></i>
 
 1) Faites des tests avec la commande `curl` afin de comprendre le fonctionnement de l'API, jouez avec les paramètres. Pour observer une réponse json utiliser la commande `jq` que vous installer avec le gestionnaire de paquet `apt`. 
    
 2) Attendez au moins 2 secondes entre chaque requête. 
    
 3) N'utilisez pas le paramètre `provided` dans votre fichier `build.sbt`. 
</div>

Afin de vous aider, voici une liste de conseils : 

1) Si vous utilisez le `spark-shell` afin de tester votre code. Pour utilisez des librairies, vous devez les télécharger via la commande `sbt`. Puis les ajouter à l'aide du paramètre `--packages <groupId:artifactId:version>,<...>` 
2) Regardez les méta données renvoyées, et jouez avec leur valeur afin de récupérer un maximum de données.
3) Collectez les statistiques par matches (il s'agit des statistiques par joueurs).
4) Pour écrire vos fichiers json, n'oubliez pas d'appliquer la méthode `toString`.

À la fin de cette étape, vous devrez avoir plusieurs fichiers `json` dans `games/` et `stats/`

#### *Transformation des données* 

L'objectif de cette étape est de joindre les deux *"types"* de données collectées et de les stocker au format `csv`.

- Sur les données des matchs : 

Il faudra réaliser un `DataFrame` avec le nombre de points marqués par l'équipe, l'id du match, le nom et l'id de l'équipe. 

- Sur les données des statistiques : 

Créez un `DataFrame` avec l'id du match, l'id de l'équipe, le nombre de points marqués (*pts*), le nombre de rebonds (*reb*), le nombre de passe décisive (*ast*) et le nombre de blocks (*blk*).

> Fusionnez les deux `DataFrame` obtenus et sauvegardez le `DataFrame` final dans un fichier `csv`. 

Si vous le souhaitez, vous pouvez créer de nouvelles colonnes avec le nom du joueur possédant le meilleur score dans les catégories ci-dessus.

Voici une liste de conseils pour cette étape :

1) Utilisez la méthode `withColumn` pour manipulez vos colonnes.
2) Sélectionnez vos colonnes avec `select`.

#### *Chargement des données*

Cette partie est facultative, mais vous pouvez charger le fichier `csv` créé dans une base de données relationnelle `MySQL` en écrivant le fichier `sql`. Pour avoir un environnement MySQL, vous pouvez utiliser le [site suivant](https://www.db-fiddle.com/) ou un conteneur [Docker](https://hub.docker.com/_/mysql) que vous verrez dans la formation.

#### *Livrables attendus*

Plusieurs concepts ne sont pas abordés dans le cours, mais il est attendu que vous faites des recherches sur internet. En effet, cela fait parti du quotidien du Data Engineer de s'informer sur toutes les méthodes disponibles. 

Le rendu minimal pour cet examen est une archive tar ou zip comportant les fichiers suivants :

- `build.sbt`
    
- `Extract.scala`

- `Transform.scala`

- Le fichier `csv` final

Votre programme doit fonctionner avec un simple appel à cette commande : **sbt run**

Pour le développement des 2 premières étapes, commencez avec une seule équipe et quelques fichiers json pour les matches et les statitistiques des joueurs mais pensez à coder afin d'ajouter les autres équipes à votre programme.

Vous pouvez aussi compresser l'ensemble du projet `sbt` utilisé, accompagné d'un `README.md` pour spécifier les commandes à lancer. Pour les fichiers demandés, certaines tâches seront répétitives, pensez à utiliser des fonctions, des classes, etc. 

N'oubliez pas d'uploader votre examen sous le format d'une archive zip ou tar, dans l'onglet `Mes Exams`, après avoir validé tous les exercices du module.

Bon courage ! 
