from celery import Celery
from celery.schedules import crontab

app = Celery('tasks', backend='redis://localhost:6379/0', broker='redis://localhost:6379/0')
# You can use this to prefix keys, but they'll still have _celery-task-meta-{id} at the end.
# So there's not really much point unless you're fighting with other applications on the same
# Redis instance.
# app.conf.result_backend_transport_options = {
#     'global_keyprefix': 'my_prefix_'
# }
app.conf.timezone = 'America/Los_Angeles'
# Use Redis as both a broker and a result backend to store results
# app.conf.broker_url = 'redis://localhost:6379/0'
# app.conf.result_backend = 'redis://localhost:6379/0'

@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    # Calls test('hello') every 10 seconds.
    sender.add_periodic_task(5.0, test.s('hello'), name='add every 10')

    # Calls test('hello') every 30 seconds.
    # It uses the same signature of previous task, an explicit name is
    # defined to avoid this task replacing the previous one defined.
    sender.add_periodic_task(10.0, test.s('hello'), name='add every 30')

    # Calls test('world') every 30 seconds
    sender.add_periodic_task(15.0, test.s('world'), expires=10)

    # Executes every Monday morning at 7:30 a.m.
    # sender.add_periodic_task(
    #     crontab(hour=7, minute=30, day_of_week=1),
    #     test.s('Happy Mondays!'),
    # )
    
    

@app.task
def add(x, y):
    return x + y

@app.task
def test(arg):
    return arg
