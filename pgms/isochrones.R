# Documentation  postgis: http://postgis.net/

library(sf)
library(osrm)
library(leaflet)

connecter <- function(user, password){
  nom <- "db_tpcarto"
  hote <- "10.233.106.178"
  port <- "5432"
  conn <- dbConnect(Postgres(), dbname=nom, host=hote, user="tpcarto", password="tpcarto",port=port)
  return(conn)
}

conn <- connecter(user, password)

query <- "SET search_path TO public;"
dbSendQuery(conn, query)

sf_reg_metro <- st_read("tutos_R/postgis/reg_francemetro_2021.gpkg")
str(sf_reg_metro)
plot(st_geometry(sf_reg_metro))

# 0- Manipuler la base de données des équipements
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
cinemaIcons <- makeIcon(iconUrl = "tutos_R/postgis/film-sharp.png", 18,18)

leaflet() %>% 
  setView(lat = 48.84864, lng = 2.34297, zoom = 15) %>% 
  addTiles() %>% 
  addMarkers(lat = 48.84864, lng = 2.34297) %>% 
  addCircles(
    lat = 48.84864, lng = 2.34297, weight = 1, radius = 1000
  ) %>% 
  addMarkers(data = cinema_1km_sorbonne %>% st_transform(4326), icon = cinemaIcons)
#On peut vérifier que les infos bpe se superposent très bien aux infos OSM

# Autre façon de répondre à la question
# On récupère tous les cinémas de la bpe
# Tout le filtrage géométrique on le fait avec sf sur R
cinemas_bpe <- sf::st_read(conn, query = "SELECT * FROM bpe21_metro WHERE TYPEQU='F303';")
str(cinemas_bpe)
# buffer autour de la sorbonne
sorbonne_buffer <- st_as_sf(data.frame(x=2.34297,y=48.84864), coords = c("x","y"), crs = 4326) %>% 
  st_transform(2154) %>% 
  st_buffer(1000)

cinema_1km_sorbonne_list <- st_within(cinemas_bpe, sorbonne_buffer)
str(cinema_1km_sorbonne_list)
cinema_1km_sorbonne <- cinemas_bpe %>% filter(lengths(cinema_1km_sorbonne_list)>0)
cinema_1km_sorbonne %>% nrow() #21 cinémas là encore

# leaflet() %>% 
#   setView(lat = 48.84864, lng = 2.34297, zoom = 15) %>% 
#   addTiles() %>% 
#   addCircles(
#     lat = 48.84864, lng = 2.34297, weight = 1, radius = 1000
#   ) %>% 
#   addPolygons(data=sorbonne_buffer %>% st_transform(4326), col = "red")

# c- On souhaite récupérer l'ensemble des boulodromes présents sur l'ensemble de la région PACA
# Pour ce faire, nous n'utiliserons pas les informations sur les zonages administratifs disponibles
# Nous utiliserons le polygône de la région PACA et la fonction ST_contains



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
