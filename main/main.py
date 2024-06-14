from merger_package.merger import GoogleAppsAuto

def app_main():
    # test
    """Set your Email, Directory link, Editors and filename here....."""
    user_email = 'robinignaciosky70@gmail.com' # please put your email here.

    #test gdrive directory
    directory_link = "https://drive.google.com/drive/folders/1obpzqT7VeD5vCUZJjqs8eNECkCibFh_6"

    editor_emails = []

    filename_combined_sheet = 'Consolidated_file'      #this will be generated with an '_' and timestamp...

    """Executions"""
    google_app_actions = GoogleAppsAuto(user_email, directory_link, editor_emails, filename_combined_sheet)
    google_app_actions.main()


if __name__ == "__main__":
    app_main()