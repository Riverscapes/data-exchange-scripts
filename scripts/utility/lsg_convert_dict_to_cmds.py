# quickie to generate list of rscli commands or list of files to combine using gdalbuldvrt -input_file_list 
# 2025-06-24 09:57:56 

x = {
    61041: {'taudem_llp_rs_id':'952fe728-28bd-4398-b33c-22b9f7d84f54',},
    115903: {'taudem_llp_rs_id':'e48edb2d-4c51-4b29-bd79-d88792ba0663',},
    156911: {'taudem_llp_rs_id':'1dc63a4f-81c2-4d04-81eb-a62151802a06',},
    237512: {'taudem_llp_rs_id':'728f760e-f054-46e0-a408-248d8ac2ff80',},
    263375: {'taudem_llp_rs_id':'1e16b38b-e58f-4dbe-a37c-ba2109afc909',},
    364256: {'taudem_llp_rs_id':'863a4d80-c0ea-4e9d-b440-ca1b6e14329c',},
    424172: {'taudem_llp_rs_id':'dc6e5fbb-56d6-43f6-bfea-c64d4c99a637',},
    481612: {'taudem_llp_rs_id':'2490f502-5465-48fb-8078-f7fe714fb598',},
    588709: {'taudem_llp_rs_id':'232e7f93-3d8f-48f3-8b26-a048fa3867cd',},
    593444: {'taudem_llp_rs_id':'e32a5b01-42fc-43e8-b484-c99fb661bf11',},
    593468: {'taudem_llp_rs_id':'73693ed4-5a60-4105-a1c1-cefe5d8e659a',},
    593481: {'taudem_llp_rs_id':'2987803c-2de4-4e59-b50c-0fa2bcdd63ec',}
    }

for huc in x.keys():
    rsid = x[huc]['taudem_llp_rs_id']
    # print (f"rscli download --id {rsid} huc-{huc}-llp --no-input")
    # print (f"huc-{huc}-llp/outputs/hand.tif")
    print (f"huc-{huc}-llp/outputs/gdal_slope.tif")


