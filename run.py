# run.py
import os
from app import create_app

config_name = os.getenv('FLASK_ENV', 'development')
app = create_app(config_name)

if __name__ == '__main__':
    # Note: Using `use_reloader=False` is important for APScheduler
    # to avoid running the scheduler twice in debug mode.
    app.run(debug=app.config['DEBUG'], use_reloader=False)