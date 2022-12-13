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

query <- 'https://data.un.org/ws/rest/data/IAEG-SDGs,DF_SDG_GLH,1.10/..EG_ACS_ELEC+EG_EGY_CLEAN+EG_FEC_RNEW+EG_EGY_PRIM+EG_IFF_RANDN+EG_EGY_RNEW.1+2+15+12+434+504+729+788+818+202+11+132+178+204+270+288+324+384+430+466+478+562+566+624+654+686+694+768+854+14+108+174+175+231+232+262+404+450+454+480+508+638+646+690+706+716+728+800+834+894+17+24+120+140+148+180+226+266+678+18+72+426+516+710+748+9+53+36+554+54+90+242+540+548+598+57+296+316+520+580+583+584+585+61+16+184+258+570+772+776+798+876+882+543+19+21+60+124+304+666+840+419+5+32+68+76+152+170+218+238+254+328+600+604+740+858+862+13+84+188+222+320+340+484+558+591+29+28+44+52+92+136+192+212+214+308+312+332+388+474+500+531+533+534+535+630+652+659+660+662+663+670+780+796+850+62+142+30+156+158+344+392+408+410+446+496+34+4+50+64+144+356+364+462+524+586+35+96+104+116+360+418+458+608+626+702+704+764+143+398+417+762+795+860+145+31+48+51+196+268+275+368+376+400+414+422+512+634+682+760+784+792+887+150+39+8+20+70+191+292+300+380+470+499+620+674+688+705+724+807+151+100+112+203+348+412+498+616+642+643+703+804+154+208+233+234+246+352+372+428+440+578+752+826+830+831+832+833+155+40+56+250+276+438+442+492+528+756+513+747+753+135+199+432+485+514+515+518+722+738+746+99036+99037+99038+99039+99040+99049..._T+U+R....._T+TRT_BIOENERGY+TRT_GEOTHERMAL+TRT_MARINE+TRT_MULTIPLE+TRT_HYDROPOWER+TRT_SOLAR+TRT_WIND.../ALL/?detail=full&startPeriod=2000-01-01&endPeriod=2020-12-31&dimensionAtObservation=TIME_PERIOD'
raw_query <- readSDMX(query) |> as.data.frame()

########## PROCESS RAW DATA (T)

# process sdg7 data
sdg7 <- raw_query |>
  mutate(m49c = as.numeric(REF_AREA)) |>
  inner_join(reference) |>
  filter(region == "Africa") |>
  select(
    region_sub,
    region_int,
    ldc,
    lldc,
    sids,
    country,
    income,
    lending,
    REF_AREA,
    obsTime,
    SERIES,
    obsValue,
    UNIT_MEASURE,
    DATA_LAST_UPDATE,
    URBANISATION,
    COMPOSITE_BREAKDOWN
  ) |>
  clean_names() |>
  mutate(
    year = as.numeric(obs_time),
    data_last_update = ymd(data_last_update),
    m49c = as.numeric(ref_area),
    series_dedup = case_when(
      series == "EG_ACS_ELEC" ~ paste(series,urbanisation),
      series %in% c("EG_EGY_RNEW","EG_IFF_RANDN") ~ paste(series, composite_breakdown),
      TRUE ~ series
    ),
    series = as_factor(series_dedup)
  ) |>
  select(-c(obs_time,series_dedup, composite_breakdown, urbanisation, unit_measure, data_last_update,ref_area)) |>
  pivot_wider(names_from = series, values_from = obs_value) |>
  clean_names() |>
  rename_at(vars(contains("trt_")), ~(sub("trt_","",.))) |>
  mutate(
    m49c = structure(m49c, label = "M49 Code"),
    year = structure(year, label = "year"),
    # Reference columns
    region_sub = structure(region_sub, label = "Sub-region Name"),
    region_int = structure(region_int, label = "Intermediate Region Name"),
    ldc = structure(ldc, label = "Least Developed Countries (LDC)"),
    lldc = structure(lldc, label = "Land Locked Developing Countries (LLDC)"),
    sids = structure(sids, label = "Small Island Developing States (SIDS)"),
    income = structure(income, label = "World Bank Income-level Classification"),
    lending = structure(lending, label = "World Bank Lending Group"),
    # SDG7 columns
    eg_egy_prim  = structure(eg_egy_prim, label = "Energy intensity level of primary energy [7.3.1]"),
    eg_egy_clean = structure(eg_egy_clean, label = "Proportion of population with primary reliance on clean fuels and technology [7.1.2]"),
    eg_fec_rnew  = structure(eg_fec_rnew, label = "Renewable energy share in the total final energy consumption [7.2.1]"),
    # elect. access
    eg_acs_elec_t = structure(eg_acs_elec_t, label = "Proportion of population with access to electricity [7.1.1], total"),
    eg_acs_elec_r = structure(eg_acs_elec_r, label = "Proportion of population with access to electricity [7.1.1], rural"),
    eg_acs_elec_u = structure(eg_acs_elec_u, label = "Proportion of population with access to electricity [7.1.1, urban]"),
    # renewables
    eg_egy_rnew_t          = structure(eg_egy_rnew_t, label = "Installed renewable electricity-generating capacity (watts per capita) [7.b.1, 12.a.1], total"),
    eg_egy_rnew_wind       = structure(eg_egy_rnew_wind, label = "Installed renewable electricity-generating capacity (watts per capita) [7.b.1, 12.a.1], wind"),
    eg_egy_rnew_solar      = structure(eg_egy_rnew_solar, label = "Installed renewable electricity-generating capacity (watts per capita) [7.b.1, 12.a.1], solar"),
    eg_egy_rnew_marine     = structure(eg_egy_rnew_marine, label = "Installed renewable electricity-generating capacity (watts per capita) [7.b.1, 12.a.1], marine"),
    eg_egy_rnew_bioenergy  = structure(eg_egy_rnew_bioenergy, label = "Installed renewable electricity-generating capacity (watts per capita) [7.b.1, 12.a.1], bioenergy"),
    eg_egy_rnew_hydropower = structure(eg_egy_rnew_hydropower, label = "Installed renewable electricity-generating capacity (watts per capita) [7.b.1, 12.a.1], hydropower"),
    eg_egy_rnew_geothermal = structure(eg_egy_rnew_geothermal, label = "Installed renewable electricity-generating capacity (watts per capita) [7.b.1, 12.a.1], geothermal"),
    # financial flows
    eg_iff_randn_t         = structure(eg_iff_randn_t, label = "International financial flows to developing countries in support of clean energy research and development and renewable energy production, including in hybrid systems [7.a.1], total"),
    eg_iff_randn_wind      = structure(eg_iff_randn_wind, label = "International financial flows to developing countries in support of clean energy research and development and renewable energy production, including in hybrid systems [7.a.1], wind"),
    eg_iff_randn_solar     = structure(eg_iff_randn_solar, label = "International financial flows to developing countries in support of clean energy research and development and renewable energy production, including in hybrid systems [7.a.1], solar"),
    eg_iff_randn_marine    = structure(eg_iff_randn_marine, label = "International financial flows to developing countries in support of clean energy research and development and renewable energy production, including in hybrid systems [7.a.1], marine"),
    eg_iff_randn_multiple  = structure(eg_iff_randn_multiple, label = "International financial flows to developing countries in support of clean energy research and development and renewable energy production, including in hybrid systems [7.a.1], multiple"),
    eg_iff_randn_bioenergy = structure(eg_iff_randn_bioenergy, label = "International financial flows to developing countries in support of clean energy research and development and renewable energy production, including in hybrid systems [7.a.1], bioenergy"),
    eg_iff_randn_hydropower = structure(eg_iff_randn_hydropower, label = "International financial flows to developing countries in support of clean energy research and development and renewable energy production, including in hybrid systems [7.a.1], hydropower"),
    eg_iff_randn_geothermal = structure(eg_iff_randn_geothermal, label = "International financial flows to developing countries in support of clean energy research and development and renewable energy production, including in hybrid systems [7.a.1], geothermal")
  ) |>
  relocate(
    ldc,
    lldc,
    sids,
    income,
    lending,
    m49c,
    region_sub,
    region_int,
    country,
    year
  )

# catalog
meta_sdg7_extra <- raw_query |>
  select(SERIES, UNIT_MEASURE, DATA_LAST_UPDATE) |>
  distinct() |>
  mutate(var = tolower(SERIES)) |>
  clean_names() |>
  select(-series)

meta_sdg7 <- sdg7 |>
  filter(m49c == 12, year == 2000) |>
  rowid_to_column("index") |>
  mutate(across(everything(), ~as.character(.x))) |>
  pivot_longer(cols = -index, names_to = "var", values_drop_na = TRUE) |>
  transmute(var, description = var_label(sdg7[var])) |>
  as.data.frame()

######### SAVE PROCESSED DATA (L)
fwrite(sdg7, here("data/sdg7.csv"))
save(sdg7, file = "data/sdg7.rda")
fwrite(meta_sdg7, here("data-raw/meta_sdg7.csv"))
fwrite(meta_sdg7_extra, here("data-raw/meta_sdg7_extra.csv"))
fwrite(raw_query, here(paste0("data-raw/query_sdg7_",Sys.Date(),".csv")))

