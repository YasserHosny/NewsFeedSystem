import pika
from contextlib import asynccontextmanager
from app.logging_config import get_logger

# Initialize logger
logger = get_logger('connection_manager')

class RabbitMQConnectionManager:
    def __init__(self, config):
        self.config = config
        self.connection_pool = []
        self.max_connections = config.get('max_connections', 5)
        
    @asynccontextmanager
    async def get_connection(self):
        if not self.connection_pool:
            connection = await self._create_connection()
            self.connection_pool.append(connection)
            
        connection = self.connection_pool.pop(0)
        try:
            yield connection
        finally:
            if len(self.connection_pool) < self.max_connections:
                self.connection_pool.append(connection)
            else:
                await self._close_connection(connection)
                
    async def _create_connection(self):
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=self.config['host'],
                    port=self.config['port'],
                    virtual_host=self.config['vhost'],
                    credentials=pika.PlainCredentials(
                        self.config['username'],
                        self.config['password']
                    )
                )
            )
            logger.info("Created new RabbitMQ connection")
            return connection
        except Exception as e:
            logger.error(f"Failed to create RabbitMQ connection: {str(e)}")
            raise 

    async def _close_connection(self, connection):
        try:
            if connection and not connection.is_closed:
                await connection.close()
                logger.info("Closed RabbitMQ connection")
        except Exception as e:
            logger.error(f"Error closing connection: {str(e)}")
            
    async def __aenter__(self):
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Cleanup connections on exit"""
        for conn in self.connection_pool:
            await self._close_connection(conn)
        self.connection_pool.clear() 