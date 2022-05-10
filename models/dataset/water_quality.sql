with water as (
    select * from `cis4400-final-project.dataset.water_quality`
),
sites as (
    select * from `cis4400-final-project.dataset.location`
)
SELECT Sample_Number, 
    Sample_Date, 
    Sample_Site, 
    Sample_class, 
    Residual_Free_Chlorine__mg_L_ as Residual_Free_Chlorine, 
    Turbidity__NTU_ as Turbidity, 
    Fluoride__mg_L_ as Fluoride, 
    Coliform__Quanti_Tray___MPN__100mL_ as Coliform, 
    E_coli_Quanti_Tray___MPN_100mL_ as E_coli,
    longitude, latitude, street_address, city, zipcode
FROM water
LEFT JOIN sites using (Sample_Site)
WHERE Sample_Date >= '2019-01-01'