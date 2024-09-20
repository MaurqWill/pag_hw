from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func
from datetime import datetime

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+mysqlconnector://root:C0dingTemp012!@localhost/factory_db'
db = SQLAlchemy(app)

class Order(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    quantity = db.Column(db.Integer, nullable=False)
    employee_id = db.Column(db.Integer, nullable=False)
    total_amount = db.Column(db.Float, nullable=False)
    date = db.Column(db.Date, nullable=False)
    product_id = db.Column(db.Integer, nullable=False)
    customer_id = db.Column(db.Integer, nullable=False)

    def to_dict(self):
        return {
            'id': self.id,
            'quantity': self.quantity,
            'employee_id': self.employee_id,
            'total_amount': self.total_amount,
            'date': self.date.isoformat() if isinstance(self.date, datetime) else self.date,  
            'product_id': self.product_id,
            'customer_id': self.customer_id
        }

class Product(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name
        }

@app.route('/orders', methods=['GET'])
def get_orders():
    try:
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 10))
        orders_query = Order.query.paginate(page=page, per_page=per_page, error_out=False)
        order_list = [order.to_dict() for order in orders_query.items]
        
        return jsonify({
            'orders': order_list,
            'total': orders_query.total,
            'page': orders_query.page,
            'pages': orders_query.pages
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/products', methods=['GET'])
def get_products():
    try:
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 10))
        products_query = Product.query.paginate(page=page, per_page=per_page, error_out=False)
        product_list = [product.to_dict() for product in products_query.items]
        
        return jsonify({
            'products': product_list,
            'total': products_query.total,
            'page': products_query.page,
            'pages': products_query.pages
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/employee_performance', methods=['GET'])
def get_employee_performance():
    try:
        results = db.session.query(
            Order.employee_id,
            func.sum(Order.quantity).label('total_quantity')
        ).group_by(Order.employee_id).all()

        return jsonify([{'employee_id': employee_id, 'total_quantity': total_quantity} for employee_id, total_quantity in results])
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/top_selling_products', methods=['GET'])
def get_top_selling_products():
    try:
        results = db.session.query(
            Order.product_id,
            func.sum(Order.quantity).label('total_quantity')
        ).group_by(Order.product_id).all()

        sorted_products = sorted(results, key=lambda x: x[1], reverse=True)
        return jsonify([{'product_id': product_id, 'total_quantity': total_quantity} for product_id, total_quantity in sorted_products])
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/customer_lifetime_value', methods=['GET'])
def get_customer_lifetime_value():
    try:
        threshold = float(request.args.get('threshold', 1000))
        results = db.session.query(
            Order.customer_id,
            func.sum(Order.total_amount).label('total_value')
        ).group_by(Order.customer_id).having(func.sum(Order.total_amount) >= threshold).all()

        return jsonify([{'customer_id': customer_id, 'total_value': total_value} for customer_id, total_value in results])
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/production_efficiency', methods=['GET'])
def get_production_efficiency():
    try:
        date = request.args.get('date')
        if not date:
            return jsonify({'error': 'Date parameter is required'}), 400

        subquery = db.session.query(
            Order.product_id,
            func.sum(Order.quantity).label('total_quantity')
        ).filter(Order.date == date).group_by(Order.product_id).subquery()

        results = db.session.query(
            subquery.c.product_id,
            subquery.c.total_quantity
        ).all()

        return jsonify([{'product_id': product_id, 'total_quantity': total_quantity} for product_id, total_quantity in results])
    except Exception as e:
        return jsonify({'error': str(e)}), 400

if __name__ == '__main__':
    app.run(debug=True)
