import os
from typing import Dict, List, Generator, Tuple
import webbrowser
import re
import concurrent.futures
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlencode, urlparse, urlunparse
import json
import threading
import hashlib
import base64
import logging
from datetime import datetime, timedelta, timezone

# We want to make inquirer optional so that we can use this module in other contexts
try:
    import inquirer
except ImportError:
    inquirer = None

import requests
from dateutil.parser import parse as dateparse
from rsxml import Logger, ProgressBar, calculate_etag
from rsxml.util import safe_makedirs
from pydex.classes.riverscapes_helpers import RiverscapesProject, RiverscapesProjectType, RiverscapesSearchParams, format_date

# Disable all the weird terminal noise from urllib3
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("urllib3").propagate = False

CHARSET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~'
LOCAL_PORT = 4721
ALT_PORT = 4723
LOGIN_SCOPE = 'openid'

AUTH_DETAILS = {
    "domain": "auth.riverscapes.net",
    "clientId": "pH1ADlGVi69rMozJS1cixkuL5DMVLhKC"
}


class RiverscapesAPIException(Exception):
    """Exception raised for errors in the RiverscapesAPI.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message="RiverscapesAPI encountered an error"):
        self.message = message
        super().__init__(self.message)


class RiverscapesAPI:
    """This class is a wrapper around the Riverscapes API. It handles authentication and provides a
    simple interface for making queries.

    If you specify a secretId and clientId then this class will use machine authentication. This is
    appropriate for development and administration tasks. Otherwise it will use a browser-based
    authentication workflow which is appropriate for end-users.
    """

    def __init__(self, stage: str = None, machine_auth: Dict[str, str] = None, dev_headers: Dict[str, str] = None):
        self.log = Logger('API')
        self.stage = stage.upper() if stage else self._get_stage_interactive()

        self.machine_auth = machine_auth
        self.dev_headers = dev_headers
        self.access_token = None
        self.token_timeout = None

        # If the RSAPI_ALTPORT environment variable is set then we use an alternative port for authentication
        # This is useful for keeping a local environment unblocked while also using this code inside a codespace
        self.auth_port = LOCAL_PORT if not os.environ.get('RSAPI_ALTPORT') else ALT_PORT

        if self.stage.upper() == 'PRODUCTION':
            self.uri = 'https://api.data.riverscapes.net'
        elif self.stage.upper() == 'STAGING':
            self.uri = 'https://api.data.riverscapes.net/staging'
        else:
            raise RiverscapesAPIException(f'Unknown stage: {stage}')

    def _get_stage_interactive(self):
        """_summary_

        Returns:
            _type_: _description_
        """
        if not inquirer:
            raise RiverscapesAPIException("Inquirer is not installed so interactive stage choosing is not possible. Either install inquirer or specify the stage in the constructor.")

        questions = [
            inquirer.List('stage', message="Which Data Exchange stage?", choices=['production', 'staging'], default='production'),
        ]
        answers = inquirer.prompt(questions)
        return answers['stage'].upper()

    def __enter__(self) -> 'RiverscapesAPI':
        """ Allows us to use this class as a context manager
        """
        self.refresh_token()
        return self

    def __exit__(self, _type, _value, _traceback):
        """Behaviour on close when using the "with RiverscapesAPI():" Syntax
        """
        # Make sure to shut down the token poll event so the process can exit normally
        self.shutdown()

    def _generate_challenge(self, code: str) -> str:
        return self._base64_url(hashlib.sha256(code.encode('utf-8')).digest())

    def _generate_state(self, length: int) -> str:
        result = ''
        i = length
        chars = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
        while i > 0:
            result += chars[int(round(os.urandom(1)[0] * (len(chars) - 1)))]
            i -= 1
        return result

    def _base64_url(self, string: bytes) -> str:
        """ Convert a string to a base64url string

        Args:
            string (bytes): this is the string to convert

        Returns:
            str: the base64url string
        """
        return base64.urlsafe_b64encode(string).decode('utf-8').replace('=', '').replace('+', '-').replace('/', '_')

    def _generate_random(self, size: int) -> str:
        """ Generate a random string of a given size

        Args:
            size (int): the size of the string to generate

        Returns:
            str: the random string
        """
        buffer = os.urandom(size)
        state = []
        for b in buffer:
            index = b % len(CHARSET)
            state.append(CHARSET[index])
        return ''.join(state)

    def shutdown(self):
        """_summary_
        """
        self.log.debug("Shutting down Riverscapes API")
        if self.token_timeout:
            self.token_timeout.cancel()

    def refresh_token(self, force: bool = False):
        """_summary_

        Raises:
            error: _description_

        Returns:
            _type_: _description_
        """
        self.log.info(f"Authenticating on Riverscapes API: {self.uri}")
        if self.token_timeout:
            self.token_timeout.cancel()

        # On development there's no reason to actually go get a token
        if self.dev_headers and len(self.dev_headers) > 0:
            return self

        if self.access_token and not force:
            self.log.debug("   Token already exists. Not refreshing.")
            return self

        # Step 1: Determine if we're machine code or user auth
        # If it's machine then we can fetch tokens much easier:
        if self.machine_auth:
            token_uri = self.uri if self.uri.endswith('/') else self.uri + '/'
            token_uri += 'token'

            options = {
                'method': 'POST',
                'url': token_uri,
                'headers': {'content-type': 'application/x-www-form-urlencoded'},
                'data': {
                    'audience': 'https://api.riverscapes.net',
                    'grant_type': 'client_credentials',
                    'scope': 'machine:admin',
                    'client_id': self.machine_auth['clientId'],
                    'client_secret': self.machine_auth['secretId'],
                },
                'timeout': 30
            }

            try:
                get_token_return = requests.request(**options).json()
                # NOTE: RETRY IS NOT NECESSARY HERE because we do our refresh on the API side of things
                # self.tokenTimeout = setTimeout(self.refreshToken, 1000 * getTokenReturn['expires_in'] - 20)
                self.access_token = get_token_return['access_token']
                self.log.info("SUCCESSFUL Machine Authentication")
            except Exception as error:
                self.log.info(f"Access Token error {error}")
                raise RiverscapesAPIException(error) from error

        # If this is a user workflow then we need to pop open a web browser
        else:
            code_verifier = self._generate_random(128)
            code_challenge = self._generate_challenge(code_verifier)
            state = self._generate_random(32)
            redirect_url = f"http://localhost:{self.auth_port}/rscli/"
            login_url = urlparse(f"https://{AUTH_DETAILS['domain']}/authorize")
            query_params = {
                "client_id": AUTH_DETAILS["clientId"],
                "response_type": "code",
                "scope": LOGIN_SCOPE,
                "state": state,
                "audience": "https://api.riverscapes.net",
                "redirect_uri": redirect_url,
                "code_challenge": code_challenge,
                "code_challenge_method": "S256",
            }
            login_url = login_url._replace(query=urlencode(query_params))
            webbrowser.open_new_tab(urlunparse(login_url))

            auth_code = self._wait_for_auth_code()
            authentication_url = f"https://{AUTH_DETAILS['domain']}/oauth/token"

            data = {
                "grant_type": "authorization_code",
                "client_id": AUTH_DETAILS["clientId"],
                "code_verifier": code_verifier,
                "code": auth_code,
                "redirect_uri": redirect_url,
            }

            response = requests.post(authentication_url, headers={"content-type": "application/x-www-form-urlencoded"}, data=data, timeout=30)
            response.raise_for_status()
            res = response.json()
            self.token_timeout = threading.Timer(
                res["expires_in"] - 20, self.refresh_token)
            self.token_timeout.start()
            self.access_token = res["access_token"]
            self.log.info("SUCCESSFUL Browser Authentication")

    def _wait_for_auth_code(self):
        """ Wait for the auth code to come back from the server using a simple HTTP server

        Raises:
            Exception: _description_

        Returns:
            _type_: _description_
        """
        class AuthHandler(BaseHTTPRequestHandler):
            """_summary_

            Args:
                BaseHTTPRequestHandler (_type_): _description_
            """

            def stop(self):
                """Stop the server
                """
                self.server.shutdown()

            def do_GET(self):
                """ Do all the server stuff here
                """
                self.send_response(200)
                self.send_header("Content-type", "text/html")
                self.end_headers()

                url = "https://data.riverscapes.net/login_success?code=JSJJSDASDOAWIDJAW888dqwdqw88"

                success_html_body = f"""
                    <html>
                        <head>
                            <title>GraphQL API: Authentication successful</title>
                            <script>
                                window.onload = function() {{
                                    window.location.replace('{url}');
                                }}
                            </script>
                        </head>
                        <body>
                            <p>GraphQL API: Authentication successful. Redirecting....</p>
                        </body>
                    </html>
                """

                self.wfile.write(success_html_body.encode('utf-8'))

                query = urlparse(self.path).query
                if "=" in query and "code" in query:
                    self.server.auth_code = dict(x.split("=")
                                                 for x in query.split("&"))["code"]
                    # Now shut down the server and return
                    self.stop()

        server = ThreadingHTTPServer(("localhost", self.auth_port), AuthHandler)
        # Keep the server running until it is manually stopped
        try:
            print("Starting server to wait for auth, use <Ctrl-C> to stop")
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        if not hasattr(server, "auth_code"):
            raise RiverscapesAPIException("Authentication failed")
        else:
            auth_code = server.auth_code if hasattr(server, "auth_code") else None
        return auth_code

    def load_query(self, query_name: str) -> str:
        """ Load a query file from the file system.

        Args:
            queryName (str): _description_

        Returns:
            str: _description_
        """
        with open(os.path.join(os.path.dirname(__file__), '..', 'graphql', 'queries', f'{query_name}.graphql'), 'r', encoding='utf-8') as queryFile:
            return queryFile.read()

    def load_mutation(self, mutation_name: str) -> str:
        """ Load a mutation file from the file system.

        Args:
            mutationName (str): _description_

        Returns:
            str: _description_
        """
        with open(os.path.join(os.path.dirname(__file__), '..',  'graphql', 'mutations', f'{mutation_name}.graphql'), 'r', encoding='utf-8') as queryFile:
            return queryFile.read()

    def get_project(self, project_id: str):
        """_summary_

        Args:
            project_id (str): _description_

        Returns:
            _type_: _description_
        """
        qry = self.load_query('getProject')
        results = self.run_query(qry, {"id": project_id})
        return results['data']['getProject']

    def search(self, search_params: RiverscapesSearchParams, progress_bar: bool = False, page_size: int = 500, sort: List[str] = None, max_results: int = None) -> Generator[Tuple[RiverscapesProject, Dict, int], None, None]:
        """ A simple function to make a yielded search on the riverscapes API

        This search has two modes: If the total number of records is less than 10,000 then it will do a single paginated query.
        If the total number of records is greater than 10,000 then it will do a date-partitioned search.
        This is because ElasticSearch pagination breaks down at 10,000 items.

        The mode used is chosen automatically based on the total number of records returned by the search.

        Args:
            query (str): _description_
            variables (Dict[str, str]): _description_

        Yields:
            Tuple[project: RiverscapeProject, stats: Dict[str, any], total: int]: the project, the stats dictionary and the total number of records
        """
        qry = self.load_query('searchProjects')
        stats = {}

        # NOTE: DO NOT CHANGE THE SORT ORDER HERE. IT WILL BREAK THE PAGINATION.
        # why not make this the default argument instead of None? LSG
        sort = sort if sort else ['DATE_CREATED_DESC']

        if not search_params or not isinstance(search_params, RiverscapesSearchParams):
            raise RiverscapesAPIException("search requires a valid RiverscapesSearchParams object")

        # First make a quick query to get the total number of records
        search_params_gql = search_params.to_gql()
        stats_results = self.run_query(qry, {"searchParams": search_params_gql, "limit": 0, "offset": 0, "sort": sort})
        overall_total = stats_results['data']['searchProjects']['total']
        stats = stats_results['data']['searchProjects']['stats']
        _prg = ProgressBar(overall_total, 30, 'Search Progress')
        self.log.debug(f"Total records: {overall_total:,} .... starting retrieval...")
        if max_results and max_results > 0:
            self.log.debug(f"   ... but max_results is set to {max_results:,} so we will stop there.")
        # Set initial to and from dates so that we can paginate through more than 10,000 recirds
        now_date = datetime.now(timezone.utc)
        createdOn = search_params_gql.get('createdOn', {})
        search_to_date = dateparse(createdOn.get('to')) if createdOn.get('to') else now_date
        search_from_date = dateparse(createdOn.get('from')) if createdOn.get('from') else None

        num_results = 1  # Just to get the loop started
        outer_counter = 0
        while outer_counter < overall_total and num_results > 0:
            search_params_gql['createdOn'] = {
                "to": format_date(search_to_date),
                "from": format_date(search_from_date) if search_from_date else None
            }
            if progress_bar:
                _prg.update(outer_counter)
            # self.log.debug(f"   Searching from {search_from_date} to {search_to_date}")
            results = self.run_query(qry, {"searchParams": search_params_gql, "limit": page_size, "offset": 0, "sort": sort})
            projects = results['data']['searchProjects']['results']
            num_results = len(projects)
            inner_counter = 0
            project = None
            for search_result in projects:
                project_raw = search_result['item']
                if progress_bar:
                    _prg.update(outer_counter + inner_counter)
                project = RiverscapesProject(project_raw)
                # if inner_counter == 0:
                #     self.log.debug(f"      First created date {project.created_date} -- {project.id}")

                yield (project, stats, overall_total, _prg)
                inner_counter += 1
                outer_counter += 1
                # This is mainly for demo purposes but if we've reached the max results then we can stop this whole thing
                if max_results and max_results > 0 and outer_counter >= max_results:
                    self.log.warning(f"Max results reached: {max_results}. Stopping search.")
                    return

            # Set the from date to the last project's created date
            if project is not None:
                # self.log.debug(f"      Last created date {project.created_date} -- {project.id}")
                search_to_date = project.created_date - timedelta(milliseconds=1)

        # Now loop over the actual pages of projects and yield them back one-by-one
        if progress_bar:
            _prg.erase()
            _prg.finish()
        self.log.debug(f"Search complete: retrieved {outer_counter:,} records")

    def process_search_results_async(self,
                                     callback: callable,
                                     search_params: RiverscapesSearchParams,
                                     progress_bar: bool = False,
                                     page_size: int = 500,
                                     sort: List[str] = None,
                                     max_results: int = None,
                                     max_workers=5
                                     ):
        """

        Considerations:
            1. Thread safety: The callback function should be thread-safe. It should not modify any shared state.
            2. Error handling: The callback function should handle any exceptions that occur during processing.
            3. Watch your network usage: If you are making network requests in the callback function, be aware of the potential for rate limiting. 3-5 threads is a good starting point.
            4. Logging: Logging may happen out of order. Consider prefixing your log messages so you can sort out which thread did what.

        Args:
            callback (callable): This is the function that will be called for each project. It should take the following arguments:
                project (RiverscapesProject): The project object
                stats (Dict[str, any]): The stats dictionary
                total (int): The total number of records
                progress (ProgressBar): The progress bar object
            search_params (RiverscapesSearchParams): SAME AS THE SEARCH FUNCTION
            progress_bar (bool, optional): SAME AS THE SEARCH FUNCTION
            page_size (int, optional): SAME AS THE SEARCH FUNCTION
            sort (List[str], optional): SAME AS THE SEARCH FUNCTION
            max_results (int, optional): SAME AS THE SEARCH FUNCTION
            max_workers (int, optional): Here is where you can set the number of workers for the ThreadPoolExecutor. Defaults to 5.
        """
        log = Logger('API')
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for project, _stats, _total, _prg in self.search(search_params, progress_bar=progress_bar, page_size=page_size, sort=sort, max_results=max_results):
                # Submit a new job only if we have not reached max_workers
                if len(futures) >= max_workers:
                    # Wait for at least one future to complete before continuing
                    done, not_done = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
                    futures = list(not_done)  # Update futures list with only incomplete futures

                # Submit the job and track the future
                future = executor.submit(callback, project, _stats, _total, _prg)
                futures.append(future)

            # Final block to ensure all futures are completed
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()  # Gather results or handle exceptions
                except Exception as e:
                    log.error(f"Project {project.id} generated an exception: {e}")
        return

    def get_project_full(self, project_id: str) -> RiverscapesProject:
        """ This gets the full project record

        This is a MUCH heavier query than what comes back from the search function. If all you need is the project metadata this is
        probably not the query for you

        Args:
            project_id (str): _description_

        Returns:
            _type_: _description_
        """
        qry = self.load_query('getProjectFull')
        results = self.run_query(qry, {"id": project_id})
        return RiverscapesProject(results['data']['project'])

    def get_project_files(self, project_id: str) -> List[Dict[str, any]]:
        """ This returns the file listing with everything you need to download project files


        Args:
            project_id (str): _description_

        Returns:
            _type_: _description_
        """
        qry = self.load_query('projectFiles')
        results = self.run_query(qry, {"projectId": project_id})
        return results['data']['project']['files']

    def get_project_types(self) -> Dict[str, RiverscapesProjectType]:
        """_summary_

        Returns:
            _type_: _description_
        """
        qry = self.load_query('projectTypes')
        offset = 0
        limit = 100
        total = -1
        results = []
        while total < 0 or offset < total:
            qry_results = self.run_query(qry, {"limit": limit, "offset": offset})
            total = qry_results['data']['projectTypes']['total']
            offset += limit
            for x in qry_results['data']['projectTypes']['items']:
                results.append(x)

        return {x['machineName']: RiverscapesProjectType(x) for x in results}

    def search_count(self, search_params: RiverscapesSearchParams):
        """ Return the number of records that match the search parameters
        Args:
            query (str): _description_
            variables (Dict[str, str]): _description_

        Returns:
            Tuple[total: int, Dict[str, any]]: the total results and the stats dictionary
        """
        qry = self.load_query('searchCount')
        if not search_params or not isinstance(search_params, RiverscapesSearchParams):
            raise RiverscapesAPIException("searchCount requires a valid RiverscapesSearchParams object")
        if search_params.keywords is not None or search_params.name is not None:
            raise RiverscapesAPIException("searchCount does not support keywords or name search parameters as you will always get a large, non-representative count because of low-scoring items")

        results = self.run_query(qry, {"searchParams": search_params.to_gql(), "limit": 0, "offset": 0})
        total = results['data']['searchProjects']['total']
        stats = results['data']['searchProjects']['stats']
        return (total, stats)

    def run_query(self, query, variables):
        """ A simple function to use requests.post to make the API call. Note the json= section.

        Args:
            query (_type_): _description_
            variables (_type_): _description_

        Raises:
            Exception: _description_

        Returns:
            _type_: _description_
        """
        headers = {"authorization": "Bearer " + self.access_token} if self.access_token else {}
        request = requests.post(self.uri, json={
            'query': query,
            'variables': variables
        }, headers=headers, timeout=30)

        if request.status_code == 200:
            resp_json = request.json()
            if 'errors' in resp_json and len(resp_json['errors']) > 0:
                # Authentication timeout: re-login and retry the query
                if len(list(filter(lambda err: 'You must be authenticated' in err['message'], resp_json['errors']))) > 0:
                    self.log.debug("Authentication timed out. Fetching new token...")
                    self.refresh_token()
                    self.log.debug("   done. Re-trying query...")
                    return self.run_query(query, variables)

            else:
                # self.last_pass = True
                # self.retry = 0
                return request.json()
        else:
            raise RiverscapesAPIException(f"Query failed to run by returning code of {request.status_code}. {query} {json.dumps(variables)}")

    def download_files(self, project_id: str, download_dir: str, re_filter: List[str] = None, force=False):
        """ From a project id get all relevant files and download them

        Args:
            project_id (_type_): _description_
            local_path (_type_): _description_
            force (bool, optional): _description_. Defaults to False.
        """

        # Fetch the project files from the API
        file_results = self.get_project_files(project_id)

        # Now filter the list of files to anything that remains after the regex filter
        filtered_files = []
        for file in file_results:
            if not 'localPath' in file:
                self.log.warning('File has no localPath. Skipping')
                continue
            # now filter the
            if re_filter is not None and len(re_filter) > 0:
                if not any([re.compile(x, re.IGNORECASE).match(file['localPath'], ) for x in re_filter]):
                    continue
            filtered_files.append(file)

        if len(filtered_files) == 0:
            self.log.warning(f"No files found for project {project_id} with the given filters: {re_filter}")
            return

        for file in filtered_files:
            local_file_path = os.path.join(download_dir, file['localPath'])
            self.download_file(file, local_file_path, force)

    def download_file(self, api_file_obj: Dict[str, any], local_path: str, force=False):
        """ NOTE: The directory for this file will be created if it doesn't exist

        Arguments:
            api_file_obj {[type]} -- The dictionary that the API returns. should include the name, md5, size etc
            local_path {[type]} -- the file's local path

        Keyword Arguments:
            force {bool} -- if true we will download regardless
        """
        file_is_there = os.path.exists(local_path) and os.path.isfile(local_path)
        etag_match = file_is_there and calculate_etag(local_path) == api_file_obj['etag']

        file_directory = os.path.dirname(local_path)

        # Anything less than 5 characters is probably a bad path
        if len(file_directory) < 5:
            raise RiverscapesAPIException(f"Invalid file path: '{local_path}'")

        if not os.path.exists(file_directory):
            safe_makedirs(file_directory)

        if force is True or not file_is_there or not etag_match:
            if not etag_match and file_is_there:
                self.log.info(f'        File etag mismatch. Re-downloading: {local_path}')
            elif not file_is_there:
                self.log.info(f'        Downloading: {local_path}')
            r = requests.get(api_file_obj['downloadUrl'], allow_redirects=True, stream=True, timeout=30)
            total_length = r.headers.get('content-length')

            dl = 0
            with open(local_path, 'wb') as f:
                if total_length is None:  # no content length header
                    f.write(r.content)
                else:
                    progbar = ProgressBar(int(total_length), 50, local_path, byte_format=True)
                    for data in r.iter_content(chunk_size=4096):
                        dl += len(data)
                        f.write(data)
                        progbar.update(dl)
                    progbar.erase()
            return True
        else:
            self.log.debug(f'        File already exists (skipping): {local_path}')
            return False


if __name__ == '__main__':
    log = Logger('API')
    gql = RiverscapesAPI(os.environ.get('RS_API_URL'))
    gql.refresh_token()
    log.debug(gql.access_token)
    gql.shutdown()  # remember to shutdown so the threaded timer doesn't keep the process alive

    gql2 = RiverscapesAPI(os.environ.get('RS_API_URL'), {
        'clientId': os.environ['RS_CLIENT_ID'],
        'secretId': os.environ['RS_CLIENT_SECRET']
    })
    gql2.refresh_token()
    log.debug(gql2.access_token)
    gql2.shutdown()  # remember to shutdown so the threaded timer doesn't keep the process alive
