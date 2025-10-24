"""
Upload multiple new projects from subfolders within a specified parent folder.
Each subfolder is expected to contain a project.rs.xml file and any associated data files.

Edit the launch.json file to set the path of the parent folder, as well as specify the owner and visibility for the new projects.
After you initiate the script you will be prompted to confirm before proceeding with the upload.

********** WARNING **********
This script can create a real mess if you use it unwisely. Pause and think before you run it.
You are advise to try it against the STAGING server first to make sure it does what you want.
*****************************

Philip Bailey (based on Matt's project_upload.py script)
2 Oct 2025
"""
import os
import time
import json
import argparse
import inquirer
from rsxml import Logger
import requests
from pydex import RiverscapesAPI, __version__


def upload_projects(riverscapes_api: RiverscapesAPI, parent_folder: str, owner: str, visibility: str, tags: list):
    """ Upload all projects found in subfolders of the specified parent folder.

    Each subfolder is expected to contain a project.rs.xml file and any associated data files.

    Args:
        riverscapes_api (RiverscapesAPI): An instance of the RiverscapesAPI class.
        parent_folder (str): The path to the parent folder containing project subfolders.
    """
    log = Logger('Upload Multiple Riverscapes Projects')
    log.title('Upload Multiple Riverscapes Projects')

    # Look recursively in the parent folder for files called project.rs.xml
    project_files = []
    for root, __dirs, files in os.walk(parent_folder):
        for filename in files:
            if filename == 'project.rs.xml':
                project_files.append(os.path.join(root, filename))

    log.info(f'Found {len(project_files)} project.rs.xml files in {parent_folder}')

    proceed = inquirer.prompt([inquirer.Confirm('continue', message=f"About to upload {len(project_files)} projects found in subfolders of {parent_folder}. Continue?", default=False)])
    if not proceed['continue']:
        log.info('Upload cancelled by user.')
        return

    # Iterate over each subfolder in the parent folder
    success_count = 0
    fail_count = 0
    for project_file in project_files:
        project_folder = os.path.dirname(project_file)
        log.info(f'Processing project folder: {project_folder}')

        try:
            # Upload the project found in this folder
            upload_project(riverscapes_api, project_file, owner, visibility, tags)
            success_count += 1
        except Exception as e:
            log.error(f'Failed to upload project in folder {project_folder}: {e}')
            fail_count += 1

    log.info(f'Upload completed: {success_count} succeeded, {fail_count} failed')


def upload_project(riverscapes_api: RiverscapesAPI, project_xml_path: str, owner: str = None, visibility: str = None, tags: list = None):
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

    # Find all files recursively inside this project folder
    project_folder = os.path.dirname(project_xml_path)
    all_project_files = []
    for root, _dirs, files in os.walk(project_folder):
        for filename in files:
            rel_dir = os.path.relpath(root, project_folder)
            rel_file = os.path.join(rel_dir, filename) if rel_dir != '.' else filename
            all_project_files.append(rel_file)

    if len(all_project_files) < 1:
        log.warning(f'No files found in project folder {project_folder}')
        return

    # Step 1: Request an upload
    # ================================================================================================================
    # Get a copy of the existing project record so we can copy the owner and visibility
    upload_params = {
        # 'token': "xxxxxxxxxxxxxxxxxxxxx" isn't needed because this is a new project update operation
        'files': all_project_files,  # Relative paths for the files
        # For now I'm faking MD5 tags. This is a little sloppy but for now it works. If you put in fake MD5 tags
        # The server will just assume they're new and treat these files as updates.
        # NOTE: THIS IS NOT APPROPRIATE IF YOU WANT TO AVOID OVERWRITING FILES THAT HAVEN'T CHANGED
        'etags': ['XXXXXXXXXXXXXXXXXXXXXXXXXXXX'] * len(all_project_files),  # We don't have etags since we just downloaded the files
        'sizes': [os.path.getsize(os.path.join(project_folder, f)) for f in all_project_files],
        # NOTE: VERY IMPORTANT: If you're updating an existing project you must set noDelete to True
        'noDelete': True,
        # Owner must be explicitly set to the same owner as the existing project.
        # 'owner': {
        #     'id': owner,
        #     'type': 'ORGANIZATION'
        # },
        # # Visibility and tags must also be explicitly set so we use the values from the existing project we just looked up
        # 'visibility': visibility,
        'tags': tags
    }

    if owner is not None:
        upload_params['owner'] = {
            'id': owner,
            'type': 'ORGANIZATION'
        }

    if visibility is not None:
        upload_params['visibility'] = visibility

    project_upload_qry = riverscapes_api.load_query('requestUploadProject')
    project_upload = riverscapes_api.run_query(project_upload_qry, upload_params)
    token = project_upload['data']['requestUploadProject']['token']

    # Step 2: Now we need to request the urls for the upload so we can start working on them
    # ================================================================================================================
    upload_urls_qry = riverscapes_api.load_query('requestUploadProjectFilesUrl')
    combined_files = project_upload['data']['requestUploadProject']['create'] + project_upload['data']['requestUploadProject']['update']
    upload_urls = riverscapes_api.run_query(upload_urls_qry, {
        'files': combined_files,
        'token': token
    })

    # Step 3: Now upload each file to the provided url
    # ================================================================================================================
    log.info(f'Received {len(upload_urls["data"]["requestUploadProjectFilesUrl"])} upload urls')
    for file_info in upload_urls["data"]["requestUploadProjectFilesUrl"]:
        rel_path = file_info["relPath"]
        url = file_info["urls"][0]
        file_path = os.path.join(project_folder, rel_path)
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
    __finalize_upload = riverscapes_api.run_query(finalize_upload_qry, {
        'token': token
    })

    if True is True:
        # Step 5: Poll the upload status until it's done. This is optional so if you're immediately moving on to a different
        # project you can skip this step. Only useful if you need to know when the project is actually available online.
        # ================================================================================================================
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

    log.info(f"Upload finalized. Project URL: https://{'staging.' if riverscapes_api.stage == 'STAGING' else ''}data.riverscapes.net/p/{project_upload['data']['requestUploadProject']['newId']}")


def main():
    """Upload multiple new projects from subfolders of a specified parent folder."""

    parser = argparse.ArgumentParser(description="Upload or update a Riverscapes project.")
    parser.add_argument('stage', type=str, help="Specify the server stage to use (default: staging)")
    parser.add_argument('parent_folder', type=str, help='The parent folder inside which are subfolders representing projects to upload')
    parser.add_argument('owner', type=str, help='The owner (user or organization) to assign the new projects to')
    parser.add_argument('visibility', type=str, help='The visibility level of the new projects (public or private)')
    parser.add_argument('--tags', type=str, help='A comma-separated list of tags to assign to the new projects', default='')
    args = parser.parse_args()

    if args.parent_folder is None or not os.path.exists(args.parent_folder):
        print('Error: parent_folder is required and must exist')
        return

    log = Logger('Project Upload')
    log.title('Riverscapes Project Upload')
    log.info(f'parent folder: {args.parent_folder}')
    log.info(f'Stage: {args.stage}')

    tags = None
    if args.tags is not None and args.tags != '':
        tags = [tag.strip() for tag in args.tags.split(',')]

    with RiverscapesAPI(stage=args.stage) as api:
        upload_projects(api, args.parent_folder, args.owner, args.visibility, tags)


if __name__ == "__main__":
    main()
