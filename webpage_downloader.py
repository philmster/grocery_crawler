# ----------------------------------------------------------------------------------------------------------------------
import datetime

from datetime import datetime
from pywebcopy import save_website

# ----------------------------------------------------------------------------------------------------------------------
def downloadWebPage(url, dirDownload):
    """
    Downloads whole webpage from the specified url.
        url: The base url to download.
        dirDownload: Directory path where the webpage should be downloaded to.
    """

    beginTime = datetime.now()
    kwargs = {"bypass_robots": True, "project_name": "recognisable-name", "load_css": False, "load_images": False,
              "load_javascript": False}
    try:
        save_website(url=url, project_folder=dirDownload, **kwargs)
    except:
        print("Downloading webpage from '{0}' failed".format(url))

    print("Total run time taken by script: {0}".format(datetime.now() - beginTime))

# ----------------------------------------------------------------------------------------------------------------------
