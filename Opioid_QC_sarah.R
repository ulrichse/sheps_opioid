library(tidyverse)
library(data.table)
library(arrow)
library(dplyr)


dat.files  <- list.files(path="C:/Users/ulric/OneDrive - Appalachian State University/Documents/RStudio/Sheps_data",
                         recursive=T, #had F
                         pattern="\\.parquet$",
                         full.names=T,
                         ignore.case=F,
                         no.. = TRUE)
getwd()
icd_codes <- fread("C:/Users/ulric/OneDrive - Appalachian State University/Documents/RStudio/ICD_Codes/all_ICD9_ICD10.csv")
ICD_10 <- fread("C:/Users/ulric/OneDrive - Appalachian State University/Documents/RStudio/ICD_Codes/all_ICD10_codes.csv")
ICD_10 <- data.frame(ICD10 = ICD_10$ICD10)
ICD_9 <- data.frame(icd9 = icd_codes$icd9)

#==============================================================================#
# Build filters
#==============================================================================#

ICD10_Code <- c("^F11")
opioid_codesONE_10 <- unique(grep(paste(ICD10_Code, collapse="|"), 
                                  ICD_10$ICD10, value=TRUE))
remove <- c("F1111", "F1113", "F1121")
opioid_codesONE_10 <- opioid_codesONE_10[!(opioid_codesONE_10 %in% remove)]


ICD10_Code <- c("^T400X", "^T401X", "^T402X", "^T403X", "^T404X", "^T406",
                "T401X5A", "T401X5D", "T402X5A", "T402X5D", "T403X5A", "T403X5D", "T404X5A", "T404X5D")
opioid_codesTWO_10 <- unique(grep(paste(ICD10_Code, collapse="|"), 
                                  ICD_10$ICD10, value=TRUE))
remove_S <- grep("s", opioid_codesTWO_10, ignore.case = TRUE)

opioid_codesTWO_10 <- opioid_codesTWO_10[-remove_S]
unused <- c("T40606A", "T40606D", "T400X6A", "T400X6D", "T402X6A", "T402X6D", "T403X6A", "T403X6D", "T404X6A", "T404X6D")
opioid_codesTWO_10 <- opioid_codesTWO_10[!(opioid_codesTWO_10 %in% unused )]


opioid_codes10 <- c(opioid_codesONE_10, opioid_codesTWO_10)


opioid_codes9 <- c("30400","30401", "30402", "30403", "30470", "30471", "30472", 
                   "30473", "30550", "30551", "30552", "30553", "96500", "96501", 
                   "96502", "96509","9701", "E8500", "E8501", "E8502", "E9350", "E9351", "E9352", "E9401")


# Combine ICD-10 and ICD-9 codes for each condition
Opioid_codes <- c(opioid_codes10, opioid_codes9)

#df <- data.frame(Opioid_codes)
#Separates Codes based on Table Defintions 
ODT_9 <- c("96500", "96501", "96502", "96509", "E8500", "E8501", "E8502")
ODT_10 <- c(opioid_codesONE_10, "T400X1A", "T400X4A", "T401X1A", "T401X4A", "T402X1A", "T402X4A", 
            "T403X1A", "T403X4A", "T404X1A", "T404X4A")
OUD_9 <- c(opioid_codes9)
OUD_10 <- c(opioid_codes10)


#==============================================================================#
# Use filters

n = 2008
output_folder <- "~/parquet"

for(i in dat.files){
  system.time(Sheps <- open_dataset(i) %>%
                select(fyear, ptcnty, zip5, agey, sex, race, source, admitdt, shepsid, diag1, diag2, diag3, diag4,
                       diag5, diag6, diag7, diag8, diag9, diag10, diag11, diag12, diag13, diag14, diag15, diag16, diag17,
                       diag18, diag19, diag20, diag21, diag22, diag23, diag24, diag25, payer1, payer2, payer3, paysub1, paysub2, paysub3, 
                       matches("^diag")) %>%
                collect() %>%
                filter(!is.na(shepsid)) %>%
                #drop_na(admitdt) %>%
                mutate(fyear = year(admitdt)) %>%
                mutate(Opioid_Definition_Table_ICD9= ifelse(rowSums(sapply(select(., starts_with("diag")), `%in%`, ODT_9)) > 0, 1, 0),
                       Opioid_Definition_Table_ICD10 = ifelse(rowSums(sapply(select(., starts_with("diag")), `%in%`, ODT_10)) > 0, 1, 0),
                       Opioid_Use_Disorder_ICD9 = ifelse(rowSums(sapply(select(., starts_with("diag")), `%in%`, OUD_9)) > 0, 1, 0),
                       Opioid_Use_Disorder_ICD10 = ifelse(rowSums(sapply(select(., starts_with("diag")), `%in%`, OUD_10)) > 0, 1, 0))
              #mutate_all(as.character)
  )
  if (n < 2011) {
    Sheps$race <- fifelse(Sheps$race == 6, 7,
                          fifelse(Sheps$race == 5, 6,
                                  fifelse(Sheps$race == 4, 5,
                                          Sheps$race)))
  }
  
  write_parquet(Sheps, file.path(output_folder, paste0("Opioid_", n, ".parquet")))
  
  print(n)
  n=n+1
}


#==============================================================================#
# Create single file
#==============================================================================#

m2008 <- read_parquet("parquet/Opioid_QC_2008.parquet")
m2009 <- read_parquet("parquet/Opioid_QC_2009.parquet")
m2010 <- read_parquet("parquet/Opioid_QC_2010.parquet")
m2011 <- read_parquet("parquet/Opioid_QC_2011.parquet")
m2012 <- read_parquet("parquet/Opioid_QC_2012.parquet")
m2013 <- read_parquet("parquet/Opioid_QC_2013.parquet")
m2014 <- read_parquet("parquet/Opioid_QC_2014.parquet")
m2015 <- read_parquet("parquet/Opioid_QC_2015.parquet")
m2016 <- read_parquet("parquet/Opioid_QC_2016.parquet")
m2017 <- read_parquet("parquet/Opioid_QC_2017.parquet")
m2018 <- read_parquet("parquet/Opioid_QC_2018.parquet")
m2019 <- read_parquet("parquet/Opioid_QC_2019.parquet")
m2020 <- read_parquet("parquet/Opioid_QC_2020.parquet")
m2021 <- read_parquet("parquet/Opioid_QC_2021.parquet")

list <- list(m2008, m2009, m2010, m2011, m2012, m2013, m2014, m2015, m2016, m2017, m2018, m2019, m2020, m2021)
combined <- lapply(list, function(df){
  df %>% filter(Opioid_Definition_Table_ICD9 == 1 | Opioid_Definition_Table_ICD10 == 1 | Opioid_Use_Disorder_ICD9 | Opioid_Use_Disorder_ICD10 )
})

combined[[13]]$zip5 <- as.integer(combined[[13]]$zip5)
combined[[13]]$ptcnty <- as.integer(combined[[13]]$ptcnty)
combined[[14]]$zip5 <- as.integer(combined[[14]]$zip5)
combined[[14]]$ptcnty <- as.integer(combined[[14]]$ptcnty)

com <- bind_rows(combined)  

write_parquet(com, "opioid_data.parquet")

entries_by_year <- com %>%
  group_by(fyear) %>%
  summarise(Count = n())

print(entries_by_year)

