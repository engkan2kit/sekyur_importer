# api/auth.py

from flask import request, jsonify
from services.auth_provider import authenticate
from services.jwt_handler import generate_jwt

def init(app):

    @app.route('/api/auth', methods=['POST'])
    def auth():
        username = request.json.get('username')
        password = request.json.get('password')
        if not username or not password:
            return jsonify({"message": "Username or password missing", "status": 400}), 400

        user_data = authenticate(username, password)
        if not user_data:
            return jsonify({"message": "Invalid credentials", "status": 400}), 400

        token = generate_jwt(payload=user_data, lifetime=60) # <--- generates a JWT with valid within 1 hour by now
        return jsonify({"data": token, "status": 200}), 200
