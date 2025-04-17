# from flask import Flask, jsonify
# import os
# from daily.price_calculator import fast_price_metrics_calculation
#
# app = Flask(__name__)
#
#
# @app.route('/')
# def home():
#     return "Price Calculator Service is running."
#
#
# @app.route('/run-price-calculation', methods=['POST'])
# def run_calculation():
#     try:
#         fast_price_metrics_calculation()
#         return jsonify({"status": "success", "message": "Price calculation completed successfully"})
#     except Exception as e:
#         return jsonify({"status": "error", "message": str(e)}), 500
#
#
# # For local development
# if __name__ == '__main__':
#     app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))