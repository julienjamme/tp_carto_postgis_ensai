# Documentation  postgis: http://postgis.net/
# Installer le package osrm si besoin
if(!"osrm" %in% rownames(installed.packages())) install.packages("osrm")

library(DBI)
library(RPostgres)
library(sf)
library(osrm)
library(leaflet)
library(dplyr)

# 0- Connexion à la base données et exploration rapide

connecter <- function(user, password){
  nom <- "db_tpcarto"
  hote <- "10.233.106.178"
  port <- "5432"
  conn <- dbConnect(Postgres(), dbname=nom, host=hote, user="tpcarto", password="tpcarto",port=port)
  return(conn)
}

conn <- connecter(user, password)

dbListTables(conn) # liste des tables présentes dans la bdd (certaines sont des tables gérées par le SGBD)
# - bpe21_metro : (geodata) BPE 2021 pour la france métro: https://www.insee.fr/fr/statistiques/3568638?sommaire=3568656
# - bpe21_XX : (geodata) La même pour chaque DOM (01, 02, 03, 04)
# - regions_metro : (geodata) Polygones des régions de métropole
# - popnaiss_com : (data) population communale 2018 + naissances 2018 (précision: données à l'arrondissemnt municipal pour PLM)

dbListFields(conn, "bpe21_metro") # les colonnes de la table bpe21_metro

# Pour interagir avec la base il suffit d'envoyer des requêtes SQL
# Par exemple, récupérer la table des équipements du département de la Manche (50):
bpe_dep50 <- dbGetQuery(conn, statement = "SELECT ID, DEPCOM, DOM, SDOM, TYPEQU FROM bpe21_metro WHERE DEP='50';")
str(bpe_dep50) # On récupère le dataframe avec les lignes et colonnes voulues
# ATTENTION, si on souhaite récupérer non pas un dataframe mais une data+géométrie, on pourrait faire:
bpe_dep50 <- dbGetQuery(conn, statement = "SELECT ID, DEPCOM, DOM, SDOM, TYPEQU, GEOMETRY FROM bpe21_metro WHERE DEP='50';")
str(bpe_dep50) # Mais les données sont récupérées sous forme d'un dataframe dont la géométrie n'est considérée comme une variable comme une autre
# POUR RECUPERER UN OBJET SPATIAL, il faut passer par la fonction d'importation de sf
bpe_dep50 <- st_read(conn, query = "SELECT ID, DEPCOM, DOM, SDOM, TYPEQU, GEOMETRY FROM bpe21_metro WHERE DEP='50';")
str(bpe_dep50) #il s'agit bien d'un objet sf et data.frame désormais sur lesquels les fonctions sf déjà vues dans les autres TP seront fonctionnelles.

plot(bpe_dep50 %>% select(dom),  cex = 0.3, pch = 16)


# RQ: Bien entendu, il est possible d'importer la table entière et de la manipuler sur R avec sf et dplyr 
# mais au risque d'une perte de performance. Comparons sur la bpe21_metro (qui n'est pas très grosse) et sur une opération de filtrage simple
system.time({
  bpe_dep50 <- st_read(conn, query = "SELECT ID, DEPCOM, DOM, SDOM, TYPEQU, GEOMETRY FROM bpe21_metro WHERE DEP='50';")
})
# user  system elapsed 
# 0.142   0.003   0.260

system.time({
  bpe_dep50 <- st_read(conn, query = "SELECT * FROM bpe21_metro;") %>% 
    filter(dep == "50") %>% 
    select(id, depcom, dom, sdom, typequ)
})
# user  system elapsed 
# 32.265   1.887  34.132 
# Ce qui est long ici c'est notamment le temps d'importation de la base complète

# 
dbGetQuery(conn, "SELECT COUNT(id) FROM BPE21_metro;") # 2326287

# Pour s'exercer à la manipulation de base (SQL)

# i- Charger la table regions_metro dans R et afficher la carte des régions métropolitaines (simple plot)

regions_metro <- st_read(conn, query = "SELECT * FROM regions_metro;")
str(sf_reg_metro)
plot(st_geometry(sf_reg_metro))

# ii- Déterminer le système de projection de la table bpe21_04 (La Réunion) de deux façons différentes
# 1ere façon avec la fonction sf::st_crs
st_read(conn, query = "SELECT * FROM bpe21_04") %>% st_crs() #2975 (UTM40S)
#2nde façon avec la fonction POSTGIS ST_SRID(colonne géométrie) => renvoie le crs de chaque élément
dbGetQuery(conn, "SELECT DISTINCT(ST_SRID(geometry)) FROM bpe21_04;")
# ou fonction Find_SRID(nom du schema, nom de la table, nom de la colonne géométrie);"
dbGetQuery(conn, "SELECT Find_SRID('public','regions_metro', 'geometry');") # ici 0

# TODO - trouver d'autres exercices assez simples pour manipuler le SQL, le sf voire du postgis


# A- Manipuler la base de données des équipements
# a- Dénombrer les maternités TYPEQU='D107' par région et trier par ordre décroissant de deux façons différentes
# On pourra remarquer qu'une façon est beaucoup plus rapide

# 1ere façon de faire: charger un sf et calculer avec le tidyverse
system.time({
  res1 <- sf::st_read(conn, query = "SELECT * FROM bpe21_metro WHERE TYPEQU='D107';") %>% 
    group_by(reg) %>% 
    summarise(n_mat = n()) %>% 
    arrange(n_mat) %>% 
    st_drop_geometry()
})
#    user  system elapsed 
# 0.635   0.026   0.883 

# 2nd façon: tout faire en SQL
system.time({
  res2 <- dbGetQuery(conn, statement = "SELECT REG, COUNT(id) FROM bpe21_metro WHERE TYPEQU='D107' GROUP BY REG ORDER BY COUNT(id);")
})
# user  system elapsed 
# 0.041   0.009   0.298 

# b- Sélectionner les cinémas (TYPEQU='F303') dans un rayon d'un 1km autour de la Sorbonne (dans le 5e arrondissemnt de Paris)
# On pourra utiliser les coordoonnées (long,lat) suivantes (lat = 48.84864, long = 2.34297) pour situer La Sorbonne.
# On prendra soin de vérifier le système de projection de la bpe.

# 1ere façon: principalement avec sf
# On filtre la bpe en SQL
cinemas_bpe <- sf::st_read(conn, query = "SELECT * FROM bpe21_metro WHERE TYPEQU='F303';")
str(cinemas_bpe)
# Le reste des opérations notamment les opérations géométriques sont réalisés avec sf sur R

# On construit un buffer de 1km (une zone tampon) autour de la sorbonne
sorbonne_buffer <- data.frame(x=2.34297,y=48.84864) %>% # df des coordonnées 
  st_as_sf(coords = c("x","y"), crs = 4326) %>% #qu'on transforme en objet sf (systeme de proj WGS84 => crs=4326)
  st_transform(2154) %>% # on reprojette en LAMBERT-93 (crs=2154)
  st_buffer(1000) # on crée la zone tampon autour du point (l'unité est le mètre ici)

str(sorbonne_buffer) # le buffer est constitué d'un unique polygône
plot(sorbonne_buffer %>% st_geometry()) # qui s'avère être un cercle

# On détermine si chaque cinéma de la bpe  appartient ou non au buffer avec la fonction st_within
cinema_1km_sorbonne_list <- st_within(cinemas_bpe, sorbonne_buffer) # on obtient une liste
# str(cinema_1km_sorbonne_list)
cinema_1km_sorbonne <- cinemas_bpe %>% filter(lengths(cinema_1km_sorbonne_list)>0)
cinema_1km_sorbonne %>% nrow() #21 cinémas


# 2nde façon: travailler en SQL avec POSTGIS

# Le système de projection d'une base postgis peut se retrouver avec la fonction Find_SRID
# qui prend trois arguments: le nom du schéma (ici public), le nom de la table et le nom de la colonne de la table correspondant à la géométrie.
(crs_bpe <- dbGetQuery(conn, "SELECT Find_SRID('public','bpe21_metro', 'geometry');"))
 
# Ici on remarque que le crs est 2154 (Lambert-93).
# Il faut donc harmoniser les coordonnées du point Sorbonne et les projeter en Lambert-93
# Pour cela on va créer un objet spatial en postgis de type POINT à partir des coordonnées WGS84 (epsg = 4326) fournies
sorbonne <- "ST_GeomFromText('POINT(2.34297 48.84864)', 4326)"
# et le reprojeter en Lambert-93. 
sorbonne <- paste0("ST_Transform(", sorbonne, ", 2154)")
# Autour de la Sorbonne on crée un buffer cad une zone tampon (ici un disque de diamètre 1km)
sorbonne_buffer <- paste0("ST_Buffer(", sorbonne ,", 1000)")

# On peut dès lors écrire la requête avec l'instruction ST_WITHIN
# ST_within(A,B) indique si une géométrie A (ici nos points de la BPE) appartient à une géométrie B (ici notre buffer)
query <- paste0(
  "SELECT bpe.* FROM bpe21_metro as bpe, ", sorbonne_buffer, " AS sorbuff 
  WHERE ST_Within(bpe.geometry, sorbuff.geometry) and TYPEQU='F303';"
)
cinema_1km_sorbonne <- sf::st_read(conn, query = query)
#on utilise sf::st_read pour récupérer un objet spatial R plutôt qu'un dataframe
str(cinema_1km_sorbonne)

nrow(cinema_1km_sorbonne) # 21 cinémas dans un rayon de 1km (à vol d'oiseau)

# Représentons tout cela sur une carte leaflet
# On récupère une icone spécifique sur https://ionic.io/ionicons (mot clé film)
cinemaIcons <- makeIcon(iconUrl = "images/film-sharp.png", 18,18)

leaflet() %>% 
  setView(lat = 48.84864, lng = 2.34297, zoom = 15) %>% 
  addTiles() %>% 
  addMarkers(lat = 48.84864, lng = 2.34297) %>% 
  addCircles(
    lat = 48.84864, lng = 2.34297, weight = 1, radius = 1000
  ) %>% 
  addMarkers(data = cinema_1km_sorbonne %>% st_transform(4326), icon = cinemaIcons)
#On peut vérifier que les infos bpe se superposent très bien aux infos OSM


# RQ: 1000m en LAMBERT-93 ce n'est pas exactement 1000m en WGS84 (zoomez sur la carte suivante)
leaflet() %>%
  setView(lat = 48.84864, lng = 2.34297, zoom = 15) %>%
  addTiles() %>%
  addCircles(
    lat = 48.84864, lng = 2.34297, weight = 1, radius = 1000
  ) %>%
  addPolygons(data=sorbonne_buffer %>% st_transform(4326), col = "red")
# Les 1000m en LAMBERT-93 ne sont pas exactement 1000m en WGS84 (1000m "réels")
# On aurait pu donc projeter la bpe en wgs84 pour tenir compte de cet écart.


# c- On souhaite récupérer l'ensemble des boulodromes (TYPEQU="F102") présents sur l'ensemble de la région PACA
# Pour ce faire, nous n'utiliserons pas les informations sur les zonages administratifs disponibles
# Nous utiliserons le polygône de la région PACA (93) et la fonction st_contains

# 1ere façon: réaliser les opérations géométriques avec R grâce au package sf
paca <- st_read(conn, query = "SELECT * FROM regions_metro WHERE code = '93';")
plot(paca %>% st_geometry())

boulodromes <- st_read(conn, query = "SELECT id, typequ, geometry FROM bpe21_metro WHERE typequ = 'F102';")
str(boulodromes)

boulodromes_paca_list <- st_contains(paca, boulodromes)
boulodromes_paca <- boulodromes %>% slice(boulodromes_paca_list[[1]])

plot(paca %>% st_geometry())
plot(boulodromes_paca %>% st_geometry(), pch = 3, cex = 0.8, add = TRUE)

# on peut vérifier le résultat en récupérant directement les boulodromes de PACA depuis la BPE
# Si des différences existent, essayez de comprendre pourquoi.
boulodromes_paca_bis <- st_read(conn, query = "SELECT id, typequ, dep, qualite_xy, geometry FROM bpe21_metro WHERE typequ = 'F102' and dep in ('04','05','06','13','83','84');")
# les deux data n'ont pas le même nb d'observations (904 vs 910)
diff <- boulodromes_paca_bis %>% mutate(version_bis = TRUE) %>% 
  st_join(
    boulodromes_paca %>% mutate(version_orig = TRUE) %>% select(-typequ), by = "id"
  ) %>% 
  filter((is.na(version_bis) | (is.na(version_orig))))

# on réucpère 6 boulodromes supplémentaires:
diff

plot(paca %>% st_geometry())
plot(boulodromes_paca %>% st_geometry(), pch = 3, cex = 0.8, add = TRUE)
plot(diff %>% st_geometry(), col = "red", pch = 3, cex = 0.8, add = TRUE)

# Pour deux d'entre eux, la géolocalisation de ces boulodromes est absurde car en pleine mer

# Vérifions plus précisément les autres cas avec une carte leaflet

leaflet() %>% 
  setView(lat = 43.8741, lng = 6.0287, zoom = 8) %>% 
  addTiles() %>% 
  addMarkers(data = diff %>% st_transform(4326)) %>% 
  addPolygons(data = paca %>% st_transform(4326), stroke = 1, color = "red")

# On observe que (lecture d'ouest en est) 
# - le polygône paca simplifie le tracé réel de la région => l'extrémité de la commune de Port-de-Bouc est hors du polygône, hors un boulodrome s'y trouve.
# - les îles du Frioul ne sont pas incluses dans le polygône paca => 1 boulodrome est situé sur ces îles
# - 3 boulodromes sont situés sur la mer (dont 1 est très éloigné)
# - 1 dernier également en limite du polygône qui "oublie" une partie du territoire

#=> insister sur la nécessaire simplication des polygones 
#=> sur l'exercice de géolocalisation qui n'est pas parfait.

# 2nde façon: réaliser le traitement géométrique avec POSTGIS et la fonction ST_contains
query <- "SELECT bpe.* FROM bpe21_metro AS BPE, regions_metro AS regions
WHERE ST_Contains(regions.geometry, bpe.geometry) and bpe.typequ='F102' and regions.code = '93';"

boulodromes_paca <- st_read(conn, query = query)
plot(boulodromes_paca %>% st_geometry())
# Cette 2nde façon est à préférer pour des objets trop lourds à charger en mémoire.

# A- Densité de maternités en France métropolitaine
maternites_metro <- sf::st_read(conn, query = "SELECT * FROM bpe21_metro WHERE TYPEQU='D107';")
str(maternites_metro)

maternites_metro %>% 
  group_by(dep) %>% 
  summarise(n_mat = n())
# Rapporter le nombre de maternités au nombre d'habitants (ou habitants de 18 à 54 ans) par département
# A suivre


# B- Isochrones

# 1- Construire les courbes isochrones d'accès à un équipement

# a- Choisir un équipement dans la base bpe21 (metro ou dom)


mater <- maternites_metro[1,]

# b- Récupérer ses coordonnées 
mater_coords <- st_coordinates(mater) %>% as.numeric

plot(st_geometry(sf_reg_metro))
points(x = mater_coords[1], y = mater_coords[2], pch = 4, lwd = 2, cex = 1.5, col = "red")

# c- Transformer ses coordonnées en WGS84 (epsg=4326)
mater_coords <- st_coordinates(mater %>% st_transform(4326)) %>% as.numeric

# d- Situer l'équipement sur une carte avec leaflet
leaflet() %>% 
  setView(lng = mater_coords[1], lat = mater_coords[2], zoom = 14) %>% 
  addTiles() %>% 
  addMarkers(lng = mater_coords[1], lat = mater_coords[2])

# e- Calculer les isochrones avec osrm::osrmIsochrone
(iso <- osrmIsochrone(
  loc = mater_coords, # coordonnées du point de référence
  breaks = seq(0,60,10), # valeurs des isochrones à calculer en minutes
  res = 100 # détermine le nombre de points utilisés (res*res) pour dessiner les isochornes 
))
str(iso)

# f- Représenter ces isochrones sous forme d'une carte choroplèthe
bks <-  sort(unique(c(iso$isomin, iso$isomax)))
pals <- hcl.colors(n = length(bks) - 1, palette = "Red-Blue", rev = TRUE)
plot(iso["isomax"], breaks = bks, pal = pals, 
     main = "Isochrones (in minutes)", reset = FALSE)
points(x = mater_coords[1], y = mater_coords[2], pch = 4, lwd = 2, cex = 1.5)

leaflet() %>% 
  setView(lng = mater_coords[1], lat = mater_coords[2], zoom = 8) %>% 
  addTiles() %>% 
  addMarkers(lng = mater_coords[1], lat = mater_coords[2]) %>% 
  addProviderTiles(
    providers$CartoDB.DarkMatter,
    options = providerTileOptions(opacity = 0.4)) %>%
  addPolygons(
    data=iso, 
    fillColor = pals,
    smoothFactor = 0.3,
    weight = 1,
    opacity = 1,
    color = "white",
    dashArray = "3",
    fillOpacity = 0.65
  ) %>% 
  addLegend(
    position="bottomleft",
    colors=pals,
    labels=rev(c("50-60","40-50",
                 "30-40","20-30","10-20", "0-10")),
    opacity = 0.6,
    title="Temps de trajet par la route (en minutes)")


# 2- Second exemple
