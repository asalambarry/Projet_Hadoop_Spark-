import logging
from datetime import datetime, timedelta

import pandas as pd
from flask import Flask, jsonify, render_template
from pymongo import MongoClient

# Configure logging with more details
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configuration MongoDB
try:
    client = MongoClient(
        host='mongodb',
        port=27017,
        username='root',
        password='example',
        authSource='admin'
    )
    db = client['moviedb']
    # Test de connexion
    db.command('ping')
    logger.info("Connecté à MongoDB")
except Exception as e:
    logger.error(f"Erreur de connexion MongoDB: {str(e)}")
    raise

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/stats')
def get_stats():
    pipeline = [
        {
            '$group': {
                '_id': None,
                'total_predictions': {'$sum': 1},
                'avg_prediction': {'$avg': '$prediction'},
                'unique_users': {'$addToSet': '$userId'}
            }
        }
    ]
    stats = list(db.predictions.aggregate(pipeline))[0]
    return jsonify({
        'total_predictions': stats['total_predictions'],
        'avg_prediction': round(stats['avg_prediction'], 2),
        'unique_users': len(stats['unique_users'])
    })

@app.route('/api/recent_predictions')
def get_recent_predictions():
    predictions = list(db.predictions.find(
        {},
        {'_id': 0}
    ).sort('timestamp', -1).limit(10))
    return jsonify(predictions)

@app.route('/api/prediction_distribution')
def get_distribution():
    predictions = list(db.predictions.find({}, {'prediction': 1, '_id': 0}))
    df = pd.DataFrame(predictions)
    distribution = df['prediction'].value_counts().sort_index()
    return jsonify({
        'labels': distribution.index.tolist(),
        'values': distribution.values.tolist()
    })

@app.route('/api/latest-recommendations')
def get_latest_recommendations():
    """Obtenir les recommandations les plus récentes avec détails"""
    try:
        latest_predictions = list(db.predictions.aggregate([
            {
                '$sort': {'timestamp': -1}
            },
            {
                '$limit': 10
            },
            {
                '$project': {
                    '_id': 0,
                    'userId': 1,
                    'movieId': 1,
                    'prediction': 1,
                    'timestamp': 1
                }
            }
        ]))

        return jsonify({
            'status': 'success',
            'data': latest_predictions,
            'count': len(latest_predictions)
        })
    except Exception as e:
        logger.error(f"Error fetching latest recommendations: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/user-recommendations/<int:user_id>')
def get_user_recommendations(user_id):
    """Recommandations personnalisées pour un utilisateur"""
    try:
        user_history = list(db.predictions.find(
            {'userId': user_id},
            {'_id': 0, 'movieId': 1, 'prediction': 1}
        ).sort('prediction', -1).limit(5))

        return jsonify({
            'user_id': user_id,
            'top_recommendations': user_history,
            'total_predictions': len(user_history)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/movie-stats/<int:movie_id>')
def get_movie_stats(movie_id):
    """Statistiques détaillées pour un film spécifique"""
    try:
        stats = db.predictions.aggregate([
            {'$match': {'movieId': movie_id}},
            {'$group': {
                '_id': None,
                'avg_rating': {'$avg': '$prediction'},
                'count_ratings': {'$sum': 1},
                'max_rating': {'$max': '$prediction'},
                'min_rating': {'$min': '$prediction'}
            }}
        ])
        result = list(stats)
        if result:
            return jsonify({
                'movie_id': movie_id,
                'statistics': result[0]
            })
        return jsonify({'error': 'Film non trouvé'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/top-movies')
def get_top_movies():
    """Route pour obtenir le top 10 des films"""
    try:
        pipeline = [
            {
                '$group': {
                    '_id': '$movieId',
                    'average_rating': {'$avg': '$prediction'},
                    'total_ratings': {'$sum': 1}
                }
            },
            {'$match': {'total_ratings': {'$gte': 5}}}, 
            {'$sort': {'average_rating': -1}},
            {'$limit': 10}
        ]

        top_movies = list(db.predictions.aggregate(pipeline))

        return jsonify({
            'status': 'success',
            'data': top_movies
        })

    except Exception as e:
        logger.error(f"Erreur lors de la récupération des top films: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/api/user-activity')
def get_user_activity():
    """Activité des utilisateurs par période"""
    try:
        now = datetime.utcnow()
        last_24h = now - timedelta(days=1)

        daily_stats = db.predictions.aggregate([
            {'$match': {'timestamp': {'$gte': last_24h}}},
            {'$group': {
                '_id': {'$hour': '$timestamp'},
                'count': {'$sum': 1}
            }},
            {'$sort': {'_id': 1}}
        ])
        return jsonify(list(daily_stats))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/rating-trends')
def get_rating_trends():
    """Tendances des notes sur les dernières 24 heures"""
    try:
        last_24h = datetime.utcnow() - timedelta(days=1)
        trends = db.predictions.aggregate([
            {'$match': {'timestamp': {'$gte': last_24h}}},
            {'$group': {
                '_id': {
                    'hour': {'$hour': '$timestamp'}
                },
                'avg_rating': {'$avg': '$prediction'},
                'count': {'$sum': 1}
            }},
            {'$sort': {'_id.hour': 1}}
        ])
        return jsonify(list(trends))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/dashboard-summary')
def get_dashboard_summary():
    """Résumé complet pour le tableau de bord"""
    try:
        now = datetime.utcnow()
        last_24h = now - timedelta(days=1)

        summary = {
            'total_predictions': db.predictions.count_documents({}),
            'active_users_24h': len(db.predictions.distinct('userId', {'timestamp': {'$gte': last_24h}})),
            'recent_activity': list(db.predictions.find(
                {},
                {'_id': 0}
            ).sort('timestamp', -1).limit(5)),
            'avg_rating': db.predictions.aggregate([
                {'$group': {'_id': None, 'avg': {'$avg': '$prediction'}}}
            ]).next()['avg']
        }
        return jsonify(summary)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    try:
        logger.info("Starting Flask application on port 5001...")
        app.run(debug=True, host='0.0.0.0', port=5001)
    except Exception as e:
        logger.error(f"Failed to start Flask: {str(e)}")
        raise