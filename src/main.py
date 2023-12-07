"""
The main process of the application.
"""

import logging

from celery.result import AsyncResult
import celery

from tasks import app

logging.basicConfig(
    handlers=[logging.StreamHandler(sys.stdout)],
    level=logging.DEBUG,
    format="%(filename)s:%(lineno)d | %(asctime)s | [%(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger()

def get_stored_tasks(app: celery.Celery) -> list[AsyncResult]:
    """
    Get all stored tasks in a Redis database.
    
    This assumes that the prefix is "celery-task-meta-*"; that is, there
    is no Redis prefix that has been manually set.
    """
    # https://stackoverflow.com/questions/72115457/how-can-i-get-results-failures-for-celery-tasks-from-a-redis-backend-when-i-don
    task_results: list[AsyncResult] = []
    # This is guaranteed
    for key in app.backend.client.scan_iter("celery-task-meta-*"):
        task_id = str(key).split("celery-task-meta-", 1)[1].replace("'", "")
        task_results.append(AsyncResult(task_id, app=app))
    return task_results

def main():
    """
    For the prototype - I think it's fine if it's just a main thread calling all the tasks
    in a giant while loop, possibly with some sleeps. Since the main thread isn't really doing
    much on the side continuously, there's basically four architectures that make sense;
    - The main thread occasionally calls tasks in a giant time.sleep while-loop, waits for them to complete, and acts
    accordingly
    - The main thread repeatedly checks the backend for recent results and acts accordingly
    - There is no main thread and the entire app is built as a gigantic Celery task
    - Celery beat triggers a celery task that writes a global variable or something that the main thread can check
    """
    for r in get_stored_tasks(app):
        # print(r.ready()) # Is it ready? (bool)
        # print(r.get()) # Get the result
        # Clean up old tasks (which throws them out of the database, apparently)
        # r.forget()
        pass
        
    # print(app.backend.client)
    # result = add.delay(4, 4)
    # print(result.get(timeout=1))

if __name__ == "__main__":
    main()
    



