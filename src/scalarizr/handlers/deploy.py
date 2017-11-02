'''
Created on Apr 6, 2011

@author: marat
'''


import logging


from scalarizr.bus import bus
from scalarizr.api import operation
from scalarizr.messaging import Messages, Queues
from scalarizr.handlers import Handler, script_executor
from scalarizr.util import deploy, dicts



def get_handlers():
    return (DeploymentHandler(), )


class DeploymentHandler(Handler):

    def __init__(self):
        super(DeploymentHandler, self).__init__()
        self._logger = logging.getLogger(__name__)
        self._log_hdlr = DeployLogHandler()
        self._script_executor = None        
        bus.on(init=self.on_init)

    def on_init(self):
        bus.on(host_init_response=self.on_host_init_response)
        
    def accept(self, message, queue, **kwds):
        return message.name == Messages.DEPLOY
        
    def on_host_init_response(self, message):
        if 'deploy' in message.body:
            self.on_Deploy(self.new_message(Messages.DEPLOY, message.body['deploy']),
                           define_operation=False,
                           raise_exc=True)

    def _exec_script(self, name=None, body=None, exec_timeout=None):
        if not self._script_executor:
            self._script_executor = script_executor.get_handlers()[0]
            
        self._logger.info('Executing %s script', name)
        kwargs = dict(name=name, body=body, exec_timeout=exec_timeout or 3600)
        self._script_executor.execute_scripts(
                scripts=(script_executor.Script(**kwargs), ), 
                event_name=name, 
                scripts_qty=1)
    
    def on_Deploy(self, message, define_operation=True, raise_exc=False):
        msg_body = dicts.encode(message.body, encoding='ascii')        
     
        def handler(op):
            try:            
                assert 'deploy_task_id' in msg_body, 'deploy task is undefined'
                assert 'source' in msg_body, 'source is undefined'
                assert 'type' in msg_body['source'], 'source type is undefined'
                assert 'remote_path' in msg_body and msg_body['remote_path'], 'remote path is undefined'
                assert 'body' in msg_body['pre_deploy_routines'] if 'pre_deploy_routines' in msg_body else True
                assert 'body' in msg_body['post_deploy_routines'] if 'post_deploy_routines' in msg_body else True
    
                self._log_hdlr.deploy_task_id = msg_body['deploy_task_id']
                self._logger.addHandler(self._log_hdlr)
    
                src_type = msg_body['source'].pop('type')
                src = deploy.Source.from_type(src_type, **msg_body['source'])
                
                if msg_body.get('pre_deploy_routines') and msg_body['pre_deploy_routines'].get('body'):
                    op.logger.info('Execute pre deploy script')
                    self._exec_script(name='PreDeploy', **msg_body['pre_deploy_routines'])
                        
                op.logger.info('Update from SCM')
                src.update(msg_body['remote_path'])
                    
                if msg_body.get('post_deploy_routines') and msg_body['post_deploy_routines'].get('body'):
                    op.logger.info('Execute post deploy script')
                    self._exec_script(name='PostDeploy', **msg_body['post_deploy_routines'])
    
                self.send_message(
                    Messages.DEPLOY_RESULT, 
                    dict(
                        status='ok', 
                        deploy_task_id=msg_body['deploy_task_id']
                    )
                )

            except (Exception, BaseException), e:
                if not raise_exc:
                    self._logger.exception(e)
                self.send_message(
                    Messages.DEPLOY_RESULT, 
                    dict(
                        status='error', 
                        last_error=str(e), 
                        deploy_task_id=msg_body['deploy_task_id']
                    )
                )
                if raise_exc:
                    raise
                
            finally:
                self._logger.removeHandler(self._log_hdlr)

        if define_operation:
            op_api = operation.OperationAPI()
            op_api.run('deploy', handler)
        else:
            handler(bus.init_op)

            
class DeployLogHandler(logging.Handler):
    def __init__(self, deploy_task_id=None):
        logging.Handler.__init__(self, logging.INFO)
        self.deploy_task_id = deploy_task_id
        self._msg_service = bus.messaging_service
        
    def emit(self, record):
        msg = self._msg_service.new_message(Messages.DEPLOY_LOG, body=dict(
            deploy_task_id = self.deploy_task_id,
            message = str(record.msg) % record.args if record.args else str(record.msg)
        ))
        self._msg_service.get_producer().send(Queues.LOG, msg)
