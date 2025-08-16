# plugins/steps/messages.py
from airflow.providers.telegram.hooks.telegram import TelegramHook

def send_telegram_failure_message(context):
    hook = TelegramHook(
        token='{8406512077:AAGmVhp8qQ-AQBOJyvazOhyvbCBVEu9Vun4}', 
        chat_id='{-4935755180}')
    run_id = context['run_id']
    task_key = context['task_instance_key_str']
    
    message = f'Исполнение Дага завершилось с ошибкой. run_id: {run_id}. задача: {task_key}'
    hook.send_message({
        'chat_id': '{-4935755180}',
        'text': message
    })
    
def send_telegram_success_message(context): #добавил сам, на уроке не было
    hook = TelegramHook(telegram_conn_id='test',
                        token='{8406512077:AAGmVhp8qQ-AQBOJyvazOhyvbCBVEu9Vun4}',
                        chat_id='{-4935755180}')
    dag = context['dag']
    run_id = context['run_id']
    
    message = f'Исполнение Дага {dag} с run_id: {run_id} прошло успешно!' 
    hook.send_message({
        'chat_id': '{-4935755180}',
        'text': message
    })
