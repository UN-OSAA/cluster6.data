library(pacman)
pacman::p_load(
  tidyverse,
  data.table,
  lubridate,
  here,
  labelled,
  janitor,
  rsdmx
)

load("data/reference.rda")

########## LOAD RAW DATA (E)

# Commodity 1: Electricity, net installed capacity of electric power
# Transactions: hydro, combustible, solar thermal and solar PV by Main activity producers and autoproducers
  # query_elct_net <- "https://data.un.org/ws/rest/data/UNSD,DF_UNDATA_ENERGY,1.0/.004+008+012+016+020+024+028+031+032+036+040+044+048+050+051+052+056+060+064+068+070+072+076+081+084+090+092+096+100+104+108+112+116+120+124+132+136+140+144+148+152+156+158+170+174+175+176+178+180+184+188+191+192+196+200+203+204+208+212+214+218+222+226+230+231+232+233+234+238+242+246+251+254+255+258+262+266+268+270+275+276+278+280+288+292+296+300+304+308+312+313+316+320+324+328+332+340+344+348+352+356+360+364+368+372+376+382+384+388+392+398+400+404+408+410+412+414+417+418+422+426+428+430+434+438+440+442+446+450+454+458+462+466+470+474+475+478+480+484+496+498+499+500+504+508+512+516+520+524+528+530+531+533+534+535+540+548+554+558+562+566+570+579+580+582+583+584+585+586+591+598+600+604+608+616+620+624+626+630+634+638+639+642+643+646+654+659+660+662+666+670+678+682+686+688+690+694+702+703+704+705+706+710+716+720+724+728+729+736+740+748+752+757+760+762+764+768+776+780+784+788+792+795+796+798+800+804+807+810+818+826+831+832+833+834+840+850+854+858+860+862+876+882+886+887+890+891+894.EC.13321+13322+13341+13342+13PV1+13ST1+13PV2+13ST2/ALL/?detail=full&startPeriod=2000-01-01&endPeriod=2020-12-31&dimensionAtObservation=TIME_PERIOD"
  # query1_raw <- readSDMX(query_elct_net) |> as_data_frame()
  # fwrite(query1, here(paste0("data-raw/query1_en_stats",Sys.Date(),".csv")))
  query1_raw <- readr::read_csv(here("data-raw/query1_en_stats2022-12-14.csv"))

# Commodity 2: Total Electricity: 7000H - Hydro, 7000S - Solar Electricity and 7000T - Thermal Electricity
  # query_total_elect <- "https://data.un.org/ws/rest/data/UNSD,DF_UNDATA_ENERGY,1.0/.004+008+012+016+020+024+028+031+032+036+040+044+048+050+051+052+056+060+064+068+070+072+076+081+084+090+092+096+100+104+108+112+116+120+124+132+136+140+144+148+152+156+158+170+174+175+176+178+180+184+188+191+192+196+200+203+204+208+212+214+218+222+226+230+231+232+233+234+238+242+246+251+254+255+258+262+266+268+270+275+276+278+280+288+292+296+300+304+308+312+313+316+320+324+328+332+340+344+348+352+356+360+364+368+372+376+382+384+388+392+398+400+404+408+410+412+414+417+418+422+426+428+430+434+438+440+442+446+450+454+458+462+466+470+474+475+478+480+484+496+498+499+500+504+508+512+516+520+524+528+530+531+533+534+535+540+548+554+558+562+566+570+579+580+582+583+584+585+586+591+598+600+604+608+616+620+624+626+630+634+638+639+642+643+646+654+659+660+662+666+670+678+682+686+688+690+694+702+703+704+705+706+710+716+720+724+728+729+736+740+748+752+757+760+762+764+768+776+780+784+788+792+795+796+798+800+804+807+810+818+826+831+832+833+834+840+850+854+858+860+862+876+882+886+887+890+891+894.7000H+7000S+7000T.01BI+01CL+01CP+01CR+01DL+01LB+01LBF+01MG+01NG+01NRW+01OS+01PP+01PT+01RF+01RW+01SBF/ALL/?detail=full&startPeriod=2000-01-01&endPeriod=2020-12-31&dimensionAtObservation=TIME_PERIOD"
  # query2_raw <- readSDMX(query_total_elect) |> as_data_frame()
  # fwrite(query2, here(paste0("data-raw/query2_en_stats",Sys.Date(),".csv")))
  query2_raw <- readr::read_csv(here("data-raw/query2_en_stats2022-12-14.csv"))

########## PROCESS RAW DATA (T)

####### Query 1

# UNIT_MEASURE: MW

# TRANSACTION     n
# <chr>       <int>
#   1 13321        3232
# 2 13322        1190
# 3 13341        4632
# 4 13342        3118
# 5 13PV1         410
# 6 13PV2         249
# 7 13ST1          37
# 8 13ST2           1

query1 <- query1_raw |>
  select(REF_AREA,COMMODITY,TRANSACTION,obsTime,obsValue) |>
  clean_names() |>
  mutate(
    label = "Electricity, net installed capacity of electric power plants",
    transaction = case_when(
      transaction == '13321' ~ 'Hydro, Main activity producers',
      transaction == '13322' ~ 'Hydro, Autoproducers',
      transaction == '13341' ~ 'Combustible Fuels, Main activity producers',
      transaction == '13342' ~ 'Combustible Fuels, Autoproducers',
      transaction == '13PV1' ~ 'Electricity generating capacity - Solar PV, Main activity producers',
      transaction == '13ST1' ~ 'Electricity generating capacity - Solar Thermal, Main activity producers',
      transaction == '13PV2' ~ 'Electricity generating capacity - Solar PV, Autoproducers',
      transaction == '13ST2' ~ 'Electricity generating capacity - Solar Thermal, Autoproducers',
      TRUE ~ transaction
    ),
    transaction = str_remove(transaction, "Electricity generating capacity - "),
  ) |>
  separate(transaction, c("transaction","producers"), sep = ",", remove = TRUE) |>
  mutate(producers = str_trim(producers, side = "both")) |>
  rename(m49c = ref_area)

######## Query 2

# UNIT_MEASURE: GWHR

# COMMODITY     n
# <chr>     <int>
#   1 7000T     18175

# TRANSACTION     n
# <chr>       <int>
#   1 01BI         1036
# 2 01CL         1499
# 3 01CP          218
# 4 01CR          231
# 5 01DL         3464
# 6 01LB          739
# 7 01LBF         251
# 8 01MG          728
# 9 01NG         2261
# 10 01NRW         861
# 11 01OS           43
# 12 01PP         1592
# 13 01PT          173
# 14 01RF         2480
# 15 01RW          703
# 16 01SBF        1896

query2 <- query2_raw |>
  select(REF_AREA,COMMODITY,TRANSACTION,obsTime,obsValue) |>
  clean_names() |>
  mutate(
    producers = NA,
    label = "Total Electricity Production",
    commodity = case_when(
      commodity == "7000T" ~ "Thermal Electricity",
      TRUE ~ commodity
    ),
    transaction = case_when(
      transaction == '01BI' ~ 'Production from biogases',
      transaction == '01CL' ~ 'Production from hard coal',
      transaction == '01CP' ~ 'Production from solid coal products',
      transaction == '01CR' ~ 'Production from crude oil',
      transaction == '01DL' ~ 'Production from gas oil/diesel oil',
      transaction == '01LB' ~ 'Production from brown coal',
      transaction == '01LBF' ~ 'Production from liquid biofuels',
      transaction == '01MG' ~ 'Production from manufactured gases',
      transaction == '01NG' ~ 'Production from natural gas',
      transaction == '01NRW' ~ 'Production from non-renewable waste',
      transaction == '01OS' ~ 'Production from oil shale',
      transaction == '01PP' ~ 'Production from other oil products',
      transaction == '01PT' ~ 'Production from peat',
      transaction == '01RF' ~ 'Production from fuel oil',
      transaction == '01RW' ~ 'Production from renewable municipal waste',
      transaction == '01SBF' ~ 'Production from solid biofuels',
      transaction == '01BS' ~ 'Production from bagasse',
      TRUE ~ transaction
    ),
    transaction = str_remove(transaction, "Production from "),
  ) |>
  rename(m49c = ref_area) |>
  relocate(producers, .after = transaction)

####### Complete and processed
en_stats <- query1 |> bind_rows(query2) |>
  mutate(m49c = as.numeric(m49c)) |>
  mutate(across(commodity:producers, ~as_factor(.x))) |>
  na_if('') |>
  left_join(reference)

####### Metadata
meta_en_stats <- en_stats |>
  select(commodity,label,transaction, producers) |>
  count(commodity,label,transaction, producers)

######### SAVE PROCESSED DATA (L)

fwrite(en_stats, here("data/en_stats.csv"))
save(en_stats, file = "data/en_stats.rda")
fwrite(meta_en_stats, here("data-raw/meta_en_stats.csv"))
