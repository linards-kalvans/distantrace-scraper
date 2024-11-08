import logging
import azure.functions as func
from load_active_events import main as load_active_events

app = func.FunctionApp()

@app.function_name(name="scrape_distantrace_by_timer")
@app.schedule(schedule="0 * * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=True) 
def timer_trigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    load_active_events()

    logging.info('Python timer trigger function executed.')

@app.function_name(name="HttpTrigger1")
@app.route(route="req")
def main(req: func.HttpRequest) -> str:
    user = req.params.get("user")
    return f"Hello, {user}!"
