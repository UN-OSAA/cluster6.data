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
query_elct_net <- "https://data.un.org/ws/rest/data/UNSD,DF_UNDATA_ENERGY,1.0/.004+008+012+016+020+024+028+031+032+036+040+044+048+050+051+052+056+060+064+068+070+072+076+081+084+090+092+096+100+104+108+112+116+120+124+132+136+140+144+148+152+156+158+170+174+175+176+178+180+184+188+191+192+196+200+203+204+208+212+214+218+222+226+230+231+232+233+234+238+242+246+251+254+255+258+262+266+268+270+275+276+278+280+288+292+296+300+304+308+312+313+316+320+324+328+332+340+344+348+352+356+360+364+368+372+376+382+384+388+392+398+400+404+408+410+412+414+417+418+422+426+428+430+434+438+440+442+446+450+454+458+462+466+470+474+475+478+480+484+496+498+499+500+504+508+512+516+520+524+528+530+531+533+534+535+540+548+554+558+562+566+570+579+580+582+583+584+585+586+591+598+600+604+608+616+620+624+626+630+634+638+639+642+643+646+654+659+660+662+666+670+678+682+686+688+690+694+702+703+704+705+706+710+716+720+724+728+729+736+740+748+752+757+760+762+764+768+776+780+784+788+792+795+796+798+800+804+807+810+818+826+831+832+833+834+840+850+854+858+860+862+876+882+886+887+890+891+894.EC.13321+13322+13341+13342+13PV1+13ST1+13PV2+13ST2/ALL/?detail=full&startPeriod=2000-01-01&endPeriod=2020-12-31&dimensionAtObservation=TIME_PERIOD"
query1 <- readSDMX(query_elct_net) |> as_data_frame() |> clean_names()

# Commodity 2: Total Electricity: 7000H - Hydro, 7000S - Solar Electricity and 7000T - Thermal Electricity
query_total_elect <- "https://data.un.org/ws/rest/data/UNSD,DF_UNDATA_ENERGY,1.0/.004+008+012+016+020+024+028+031+032+036+040+044+048+050+051+052+056+060+064+068+070+072+076+081+084+090+092+096+100+104+108+112+116+120+124+132+136+140+144+148+152+156+158+170+174+175+176+178+180+184+188+191+192+196+200+203+204+208+212+214+218+222+226+230+231+232+233+234+238+242+246+251+254+255+258+262+266+268+270+275+276+278+280+288+292+296+300+304+308+312+313+316+320+324+328+332+340+344+348+352+356+360+364+368+372+376+382+384+388+392+398+400+404+408+410+412+414+417+418+422+426+428+430+434+438+440+442+446+450+454+458+462+466+470+474+475+478+480+484+496+498+499+500+504+508+512+516+520+524+528+530+531+533+534+535+540+548+554+558+562+566+570+579+580+582+583+584+585+586+591+598+600+604+608+616+620+624+626+630+634+638+639+642+643+646+654+659+660+662+666+670+678+682+686+688+690+694+702+703+704+705+706+710+716+720+724+728+729+736+740+748+752+757+760+762+764+768+776+780+784+788+792+795+796+798+800+804+807+810+818+826+831+832+833+834+840+850+854+858+860+862+876+882+886+887+890+891+894.7000H+7000S+7000T.01BI+01CL+01CP+01CR+01DL+01LB+01LBF+01MG+01NG+01NRW+01OS+01PP+01PT+01RF+01RW+01SBF/ALL/?detail=full&startPeriod=2000-01-01&endPeriod=2020-12-31&dimensionAtObservation=TIME_PERIOD"
query2 <- readSDMX(query_total_elect) |> as_data_frame() |> clean_names()

# raw_query_func <- function(source = "saved"){
#   if(source == "saved"){
#     read_csv(here("data-raw/query_sdg7_2022-12-07.csv"))
#   } else{
#     readSDMX(query) |> as_data_frame()
#   }
# }
#
# raw_query <- raw_query_func()

########## PROCESS RAW DATA (T)

######## Query 1

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


# ######## Query 2

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











# catalog
# meta_sdg7_extra <- raw_query |>
#   select(SERIES, UNIT_MEASURE, DATA_LAST_UPDATE) |>
#   distinct() |>
#   mutate(var = tolower(SERIES)) |>
#   clean_names() |>
#   select(-series)

# meta_sdg7 <- sdg7 |>
#   filter(m49c == 12, year == 2000) |>
#   rowid_to_column("index") |>
#   mutate(across(everything(), ~as.character(.x))) |>
#   pivot_longer(cols = -index, names_to = "var", values_drop_na = TRUE) |>
#   transmute(var, description = var_label(sdg7[var])) |>
#   as.data.frame()

######### SAVE PROCESSED DATA (L)

# fwrite(sdg7, here("data/sdg7.csv"))
# save(sdg7, file = "data/sdg7.rda")
# fwrite(meta_sdg7, here("data-raw/meta_sdg7.csv"))
# fwrite(meta_sdg7_extra, here("data-raw/meta_sdg7_extra.csv"))

fwrite(query1, here(paste0("data-raw/query1_en_stats",Sys.Date(),".csv")))
fwrite(query2, here(paste0("data-raw/query2_en_stats",Sys.Date(),".csv")))

# raw_query_save <- function(source = "saved"){
#   if(source == "saved"){
#     fwrite(raw_query, here("data-raw/query_sdg7_2022-12-07.csv"))
#   }else{
#     fwrite(raw_query, here(paste0("data-raw/query_sdg7_",Sys.Date(),".csv")))
#   }
# }
#
# raw_query_save()
