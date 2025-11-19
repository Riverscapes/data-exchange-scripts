# quickie to generate list of rscli commands or list of files to combine using gdalbuldvrt -input_file_list 
# 2025-06-24 09:57:56 

x = {
    
593472: {'taudem_llp_rs_id': '9b91cc92-d35c-4f79-bb67-a08b9f79912e' },
593451: {'taudem_llp_rs_id': '85a88eed-1cc8-4e05-a690-ea07f6745242' }

    }

for huc in x.keys():
    rsid = x[huc]['taudem_llp_rs_id']
    # print (f"rscli download --id {rsid} huc-{huc}-llp --no-input")
    # print (f"huc-{huc}-llp/outputs/hand.tif")
    print (f"huc-{huc}-llp/outputs/gdal_slope.tif")


