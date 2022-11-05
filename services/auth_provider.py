# services/auth_provider.py

def authenticate(username, password):
    if username == 'admin' and password == 'password':
        return {
            'username': 'admin',
            'email': 'admin@admin.com',
            'roles': ['admin', 'user']
        }
    else:
        return False
