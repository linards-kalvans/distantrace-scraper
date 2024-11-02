import logging
import azure.functions as func
from load_active_events import main as load_active_events

app = func.FunctionApp()

@app.schedule(schedule="0 0 4 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def timer_trigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    load_active_events()

    logging.info('Python timer trigger function executed.')