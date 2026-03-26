"""Change the avatar for the currently authenticated user on the Riverscapes Data Exchange."""
import os
import time
import json
import questionary
import requests
from rsxml import Logger
from pydex import RiverscapesAPI

ALLOWED_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.webp'}
MAX_POLL_ATTEMPTS = 30
POLL_INTERVAL = 5


def change_user_avatar(riverscapes_api: RiverscapesAPI):
    """Upload an image and set it as the authenticated user's avatar.

    NOTE: The updateUser mutation always updates the currently logged-in
    user's profile. The user ID prompt is used only to validate the entity
    exists for requestUploadImage.
    """
    log = Logger('ChangeUserAvatar')
    log.title('Change User Avatar')

    # Prompt for the user ID whose avatar we want to change
    user_id = questionary.text("User ID to update avatar for:").ask()
    if not user_id or not user_id.strip():
        log.info("No user ID provided. Aborting.")
        return
    user_id = user_id.strip()
    log.info(f"Updating avatar for user: {user_id}")

    # 1. Prompt for an image path and validate it
    image_path = questionary.path(
        "Path to the new avatar image:",
        only_directories=False,
    ).ask()

    if not image_path:
        log.info("No image path provided. Aborting.")
        return

    image_path = os.path.expanduser(image_path.strip())
    if not os.path.isfile(image_path):
        log.error(f"File not found: {image_path}")
        return

    ext = os.path.splitext(image_path)[1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        log.error(f"Unsupported image type '{ext}'. Allowed: {', '.join(sorted(ALLOWED_EXTENSIONS))}")
        return

    # 2. Request a signed upload URL for the user's avatar
    request_upload_qry = riverscapes_api.load_query('requestUploadImage')
    upload_resp = riverscapes_api.run_query(request_upload_qry, {
        'entityId': user_id,
        'entityType': 'USER',
    })

    if not upload_resp or 'data' not in upload_resp or 'requestUploadImage' not in upload_resp['data']:
        log.error("Failed to obtain a signed upload URL.")
        return

    data = upload_resp['data']['requestUploadImage']
    token = data['token']
    fields = data['fields']
    signed_url = data['url']

    # 3. Upload the image to the signed URL
    log.info(f"Uploading {os.path.basename(image_path)} to {signed_url}...")
    with open(image_path, 'rb') as img_file:
        response = requests.post(signed_url, data=fields, files={'file': img_file}, timeout=60)

    if response.status_code not in (200, 201, 204):
        log.error(f"Upload to S3 failed with status {response.status_code}: {response.text}")
        return

    log.info(f"Upload complete (HTTP {response.status_code}). Waiting for processing...")

    # 4. Poll checkUpload until the image is processed
    #    The upload-manager Lambda needs time to trigger and resize the image.
    #    UNKNOWN means index.json hasn't been written yet (Lambda still starting).
    check_qry = riverscapes_api.load_query('checkUpload')
    time.sleep(POLL_INTERVAL)

    for attempt in range(1, MAX_POLL_ATTEMPTS + 1):
        check_resp = riverscapes_api.run_query(check_qry, {'token': token})
        check_data = check_resp['data']['checkUpload']
        status = check_data['status']
        log.info(f"  checkUpload attempt {attempt}/{MAX_POLL_ATTEMPTS}: status={status}")

        if status in ('PROCESSING', 'UNKNOWN'):
            # UNKNOWN = Lambda hasn't written index.json yet; keep waiting
            time.sleep(POLL_INTERVAL)
            continue
        elif status == 'SUCCESS':
            log.info("Image processed. Updating user avatar...")
            # 5. Now that the image is ready, set it as the avatar
            update_user_mut = riverscapes_api.load_mutation('updateUser')
            riverscapes_api.run_query(update_user_mut, {
                'userId': user_id,
                'profile': {'avatarToken': token}
            })
            log.info(f"Avatar updated successfully for user {user_id}!")
            return
        elif status == 'FAILED':
            errors = check_data.get('errors')
            log.error(f"Image processing failed: {errors}")
            return
        else:
            log.error(f"Unexpected status: {status}")
            log.error(json.dumps(check_data, indent=2))
            return

    log.error(f"Timed out after {MAX_POLL_ATTEMPTS * POLL_INTERVAL}s waiting for image processing.")


if __name__ == '__main__':
    with RiverscapesAPI() as api:
        change_user_avatar(api)
