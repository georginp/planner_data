def extract_info_createdBy(created_by):
    user_info = created_by.get("user", {})
    application_info = created_by.get("application", {})
    return user_info, application_info


def extract_values_user(created_by_user):
    display_name = created_by_user.get('displayName', None)
    user_id = created_by_user.get('id', None)
    return display_name, user_id


def extract_values_application(created_by_application):
    display_name = created_by_application.get('displayName', None)
    user_id = created_by_application.get('id', None)
    return display_name, user_id
