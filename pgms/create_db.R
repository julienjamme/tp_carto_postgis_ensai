library(DBI)
library(RPostgres)
library(Rpos)
library(dplyr)
library(dbplyr)
library(sf)

# source("user_pass.R", encoding = "UTF-8")

connecter <- function(user, password){
  nom <- "db_tpcarto"
  hote <- "10.233.106.178"
  port <- "5432"
  conn <- dbConnect(Postgres(), dbname=nom, host=hote, user="tpcarto", password="tpcarto",port=port)
  return(conn)
}

conn <- connecter(user, password)

DBI::dbListObjects(conn)

# Téléchargement de la BPE 2021
temp <- tempfile()
download.file("https://www.insee.fr/fr/statistiques/fichier/3568638/bpe21_ensemble_xy_csv.zip",temp)
bpe21 <- readr::read_delim(unz(temp, "bpe21_ensemble_xy.csv"), delim = ";", col_types = readr::cols(LAMBERT_X="n",LAMBERT_Y="n",.default="c"))
str(bpe21)
unlink(temp)
table(is.na(bpe21$LAMBERT_X))
table(is.na(bpe21$LAMBERT_Y))

# On transforme le data en un objet sf (avec géométrie)
sf_bpe21_metro <- bpe21 %>% 
  na.omit() %>% 
  filter(REG > 10) %>% 
  mutate(id = 1:n()) %>% 
  relocate(id) %>% 
  sf::st_as_sf(coords = c("LAMBERT_X","LAMBERT_Y"), crs = 2154)

str(sf_bpe21_metro)  



# Construction de la requête créant les tables
types_vars_metro <- purrr::map_chr(
  names(sf_bpe21_metro)[-c(1,24)],
  function(var){
    paste0(var, " VARCHAR(", max(nchar(sf_bpe21_metro[[var]])), "), ")
  }
) %>% 
  paste0(., collapse="")

query <- paste0(
  'CREATE TABLE bpe21_metro',
  '( id INT PRIMARY KEY,',
  types_vars_metro,
  'geometry GEOMETRY(POINT, 2154));',
  collapse =''
)

# Création de la table
dbSendQuery(conn, query)
dbListTables(conn)

# Remplissage avec la table metro
sf::st_write(
  obj = sf_bpe21_metro %>% rename_with(tolower),
  dsn = conn,
  Id(table = 'bpe21_metro'),
  append = TRUE
)

# test lecture
bpe_head<- sf::st_read(conn, query = 'SELECT * FROM bpe21_metro LIMIT 10;')
str(bpe_head)
st_crs(bpe_head)

# Pour les DOM
sf_bpe21_doms <- purrr::map2(
  c("01","02","03","04"), #pas de données géolocalisées sur Mayotte
  c(5490,5490,2972,2975),
  function(reg,epsg){
    bpe21 %>% 
      filter(REG == reg) %>% 
      na.omit() %>% 
      mutate(id = 1:n()) %>% 
      relocate(id) %>% 
      sf::st_as_sf(coords = c("LAMBERT_X","LAMBERT_Y"), crs = epsg)
  }
)
names(sf_bpe21_doms) <- c("01","02","03","04")

purrr::walk2(
  c("01","02","03","04"), #pas de données géolocalisées sur Mayotte
  c(5490,5490,2972,2975),
  function(reg,epsg){
    nom_table <- paste0('bpe21_',reg)
    types_vars <- purrr::map_chr(
      names(sf_bpe21_doms[[reg]])[-c(1,24)],
      function(var){
        paste0(var, " VARCHAR(", max(nchar(sf_bpe21_doms[[reg]][[var]])), "), ")
      }
    ) %>% 
      paste0(., collapse="")
    
    query <- paste0(
      'CREATE TABLE ',nom_table,
      '( id INT PRIMARY KEY,',
      types_vars,
      'geometry GEOMETRY(POINT, ', epsg,'));',
      collapse =''
    )
    
    sf::st_write(
      obj = sf_bpe21_doms[[reg]] %>% rename_with(tolower),
      dsn = conn,
      Id(table = nom_table),
      append = FALSE
    )
  }
)
dbListTables(conn)

bpe_head<- sf::st_read(conn, query = 'SELECT * FROM bpe21_04 LIMIT 10;')
str(bpe_head)
st_crs(bpe_head)

dbDisconnect(conn)


# Ajout d'une table communale

temp <- tempfile()
download.file("https://www.insee.fr/fr/statistiques/fichier/5395878/BTT_TD_POP1A_2018.zip",temp)
popcom <- readr::read_delim(unz(temp, "BTT_TD_POP1A_2018.CSV"), delim = ";",col_types = list(NB="n",.default="c"))
str(popcom)
unlink(temp)

temp <- tempfile()
download.file("https://www.insee.fr/fr/statistiques/fichier/1893255/base_naissances_2021_csv.zip",temp)
naisscom <- readr::read_delim(unz(temp, "base_naissances_2021.csv"), delim = ";")
str(naisscom)
unlink(temp)

naisscom %>% filter(CODGEO == "75056")
naisscom %>% filter(CODGEO == "75101")
naisscom %>% filter(CODGEO == "75118")
naisscom %>% filter(CODGEO == "13055")
naisscom %>% filter(CODGEO == "69123")
# Répartition des naissances communales dans les arrondissements municipaux au prorata des naissances déjà observées

naiss_paris <- naisscom %>% filter(CODGEO == "75056") %>% pull(NAISD21)
paris_ratio <- naisscom %>% 
  filter(substr(CODGEO,1,3) == 751) %>% 
  select(CODGEO, NAISD21) %>% 
  mutate(ratio = NAISD21/sum(NAISD21)) %>% 
  mutate(NAISD21_ADD = naiss_paris*ratio)

naiss_mars <- naisscom %>% filter(CODGEO == "13055") %>% pull(NAISD21)
mars_ratio <- naisscom %>% 
  filter(substr(CODGEO,1,3) == 132) %>% 
  select(CODGEO, NAISD21) %>% 
  mutate(ratio = NAISD21/sum(NAISD21)) %>% 
  mutate(NAISD21_ADD = naiss_mars*ratio)

naiss_lyon <- naisscom %>% filter(CODGEO == "69123") %>% pull(NAISD21)
lyon_ratio <- naisscom %>% 
  filter(substr(CODGEO,1,4) == 6938) %>% 
  select(CODGEO, NAISD21) %>% 
  mutate(ratio = NAISD21/sum(NAISD21)) %>% 
  mutate(NAISD21_ADD = naiss_lyon*ratio)

popnaiss_com <- popcom %>% 
  group_by(CODGEO) %>%
  summarise(POP = sum(NB), .groups = 'drop') %>% 
  full_join(
    popcom %>% 
  group_by(CODGEO, SEXE) %>%
  summarise(NB = sum(NB), .groups = 'drop') %>% 
  mutate(SEXE = case_when(SEXE==1~"SEXE_H",TRUE~"SEXE_F")) %>% 
  tidyr::pivot_wider(names_from = SEXE, values_from = NB, values_fill = 0),
  by = "CODGEO"
) %>% 
  full_join(
    popcom %>% 
      group_by(CODGEO, AGEPYR10) %>%
      summarise(NB = sum(NB), .groups = 'drop') %>% 
      mutate(AGEPYR10 = paste0("AGE_", ifelse(as.numeric(AGEPYR10) < 10, paste0("0",AGEPYR10), AGEPYR10))) %>% 
      arrange(CODGEO, AGEPYR10) %>% 
      tidyr::pivot_wider(names_from = AGEPYR10, values_from = NB, values_fill = 0),
    by = "CODGEO"
  ) %>%
  left_join(
    popcom %>%  select(1:3) %>% unique(), by = "CODGEO"
  ) %>% 
  relocate(CODGEO, NIVGEO, LIBGEO) %>% 
  full_join(
    naisscom %>% 
      full_join(
        bind_rows(bind_rows(paris_ratio, mars_ratio), lyon_ratio)
      ) %>% 
      tidyr::replace_na(list(NAISD21_ADD = 0)) %>% 
      mutate(NAISD21_AJ = NAISD21 + NAISD21_ADD) %>% 
      select(CODGEO, NAISD21_AJ), 
    by = "CODGEO"
  ) %>% 
  filter(! CODGEO %in% c("75056","13055","69123")) %>% 
  filter(substr(CODGEO,1,2) < 97) %>% 
  filter(!is.na(NAISD21_AJ)) # pb coherence geo entre les deux tables (10 communes concernées)

# données départementales
popnaiss_dep <- popnaiss_com %>% 
  mutate(DEP = substr(CODGEO,1,2)) %>% 
  group_by(DEP) %>% 
  summarise(across(where(is.numeric), sum))


# Ajout de fonds de polygones

# Recupération sur Minio

sf_reg_metro <- aws.s3::s3read_using(
  FUN = sf::st_read,
  layer = "commune_francemetro_2021.shp",
  drivers = "ESRI Shapefile",
  # Mettre les options de FUN ici
  object = "/fonds/commune_francemetro_2021.shp",
  bucket = "julienjamme",
  opts = list("region" = "")
)
sf_reg_metro %>% str()

