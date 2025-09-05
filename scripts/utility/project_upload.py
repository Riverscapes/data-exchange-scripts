import os
import time
import json
from rsxml import Logger
from rsxml.util import safe_makedirs
import requests
import inquirer
from pydex import RiverscapesAPI

PROJECT_ID = '81d118ac-4f2d-42b8-bfc9-3149aebed2c5'  # This is a real project on the STAGING server


def upload_project_files(riverscapes_api: RiverscapesAPI):
    """ A typical pattern we use is to upload or update files in a project. In order to do this we need to upload both the
    files we wish to change as well as the project.rs.xml file which describes the project and its files.

    For this project we're going to upload an overwrite a real project on the STAGING server
        https://staging.data.riverscapes.net/p/81d118ac-4f2d-42b8-bfc9-3149aebed2c5/

    The upload workflow is as follows:
        1. Request an upload which gives us a token and a list of files to upload
        2. Request upload urls for each file
        3. Upload each file to the provided url using python's requests library
        4. Finalize the upload. This will tell the server to start processing the uploaded files
        5. Poll the upload status until it's done (this one is optional but nice to have)

    """
    log = Logger('Upload Riverscapes Files')
    log.title('Upload Riverscapes Files')

    # This part is not actually uploading. We're just downloading the existing project files so we can modify them
    # ================================================================================================================
    # The download dir is the /data folder in this repo
    download_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'upload_project_files'))
    # Create the directory recursively if it doesn't exist
    safe_makedirs(download_dir)
    # Download the project.rs.xml and project_bounds.geojson files to the download_dir
    riverscapes_api.download_files(PROJECT_ID,
                                   download_dir=download_dir,
                                   re_filter=[r'project\.rs\.xml', r'project_bounds\.geojson'],
                                   force=True)

    log.info(f'Downloaded project.rs.xml and project_bounds.json to {download_dir}')

    # Put in a little inquirer pause so the user can modify the files if they want
    inquirer.prompt([inquirer.Confirm('continue', message="Modify the files in the download directory if you want, then hit enter to continue", default=True)])

    # Step 1: Request an upload
    # ================================================================================================================
    # Get a copy of the existing project record so we can copy the owner and visibility
    existing_project = riverscapes_api.get_project_full(PROJECT_ID)
    upload_params = {
        'projectId': PROJECT_ID,
        # 'token': "xxxxxxxxxxxxxxxxxxxxx" isn't needed because this is a new project update operation
        'files': ['project.rs.xml', 'project_bounds.geojson'],  # Relative paths for the files
        # For now I'm faking MD5 tags. This is a little sloppy but for now it works. If you put in fake MD5 tags
        # The server will just assume they're new and treat these files as updates.
        # NOTE: THIS IS NOT APPROPRIATE IF YOU WANT TO AVOID OVERWRITING FILES THAT HAVEN'T CHANGED
        'etags': ['XXXXXXXXXXXXXXXXXXXXXXXXXXXX', 'XXXXXXXXXXXXXXXXXXXXXXXXXXXX'],  # We don't have etags since we just downloaded the files
        'sizes': [os.path.getsize(os.path.join(download_dir, 'project.rs.xml')),
                  os.path.getsize(os.path.join(download_dir, 'project_bounds.geojson'))],
        # NOTE: VERY IMPORTANT: If you're updating an existing project you must set noDelete to True
        'noDelete': True,
        # Owner must be explicitly set to the same owner as the existing project.
        'owner': {
            'id': existing_project.json['ownedBy']['id'],
            'type': existing_project.json['ownedBy']['__typename'].upper()
        },
        # Visibility and tags must also be explicitly set so we use the values from the existing project we just looked up
        'visibility': existing_project.json['visibility'],
        'tags': existing_project.json.get('tags', []),  # Tags are optional
    }
    project_upload_qry = riverscapes_api.load_query('requestUploadProject')
    project_upload = riverscapes_api.run_query(project_upload_qry, upload_params)
    token = project_upload['data']['requestUploadProject']['token']

    # Step 2: Now we need to request the urls for the upload so we can start working on them
    # ================================================================================================================
    upload_urls_qry = riverscapes_api.load_query('requestUploadProjectFilesUrl')
    upload_urls = riverscapes_api.run_query(upload_urls_qry, {
        'files': project_upload['data']['requestUploadProject']['update'],
        'token': token
    })

    # Step 3: Now upload each file to the provided url
    # ================================================================================================================
    log.info(f'Received {len(upload_urls["data"]["requestUploadProjectFilesUrl"])} upload urls')
    for file_info in upload_urls["data"]["requestUploadProjectFilesUrl"]:
        rel_path = file_info["relPath"]
        url = file_info["urls"][0]
        file_path = os.path.join(download_dir, rel_path)
        print(f"Uploading {file_path} to {url.split('?')[0]} ...")

        with open(file_path, "rb") as f:
            response = requests.put(url, data=f, timeout=120)
            if response.status_code == 200:
                print(f"Successfully uploaded {rel_path}")
            else:
                print(f"Failed to upload {rel_path}: {response.status_code} {response.text}")

    # Step 4: Now that all files are uploaded we need to finalize the upload
    # ================================================================================================================
    finalize_upload_qry = riverscapes_api.load_mutation('finalizeProjectUpload')
    finalize_upload = riverscapes_api.run_query(finalize_upload_qry, {
        'token': token
    })

    # Step 5: Poll the upload status until it's done. This is optional so if you're immediately moving on to a different
    # project you can skip this step. Only useful if you need to know when the project is actually available online.
    # ================================================================================================================
    log.info(f"Upload finalized. Project URL: https://{'staging.' if riverscapes_api.stage == 'staging' else ''}data.riverscapes.net/p/{PROJECT_ID}")
    done = False
    while not done:
        status_qry = riverscapes_api.load_query('checkUpload')
        status = riverscapes_api.run_query(status_qry, {'token': token})
        upload_status = status['data']['checkUpload']
        if upload_status['status'] in ['SUCCESS']:
            done = True
            log.info("Upload process complete")
        elif upload_status['status'] in ['FAILED']:
            log.info(f"Upload failed: {json.dumps(upload_status, indent=2)}")
            done = True
        else:
            log.info(f"...Upload status: {upload_status['status']}: Waiting 5 seconds to check status again...")
            time.sleep(5)


if __name__ == "__main__":
    with RiverscapesAPI(stage='staging') as api:
        upload_project_files(api)
