'''
Created on Feb 25, 2011
 
@author: uty
'''
 
 
from scalarizr.util.cryptotool import pwgen
from scalarizr.services.rabbitmq import rabbitmq as rabbitmq_svc
from scalarizr import rpc
 
 
class RabbitMQAPI:
 
    @rpc.command_method
    def reset_password(self, new_password=None):
        """
        Reset password for RabbitMQ user 'scalr_master'. Return new password
        """
        if not new_password:
            new_password = pwgen(10)
        rabbitmq_svc.check_master_user(new_password)
        return new_password
 
