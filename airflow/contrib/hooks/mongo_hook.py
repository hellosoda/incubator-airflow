# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
MongoHook
"""
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

from airflow.hooks.base_hook import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin

class MongoHook(BaseHook, LoggingMixin):
    """
    Hook for interacting with a MongoDB node or cluster using Pymongo. More
    information can be found at:

    https://api.mongodb.com/python/current/api/index.html
    """

    def __init__(self,
                 mongo_conn_id='mongo_default',
                 reuse_connection=True):
        self.mongo_conn_id = mongo_conn_id
        self.reuse_connection = reuse_connection
        self.mongo_client = None

        conn = self.get_connection(self.mongo_conn_id)

        host = conn.host
        port = int(conn.port)
        username = conn.login
        password = conn.password
        schema = conn.schema

        uri = 'mongodb://'

        if conn.login:
            uri += conn.login
            if conn.password:
                uri += ':' + conn.password
            uri += '@'

        uri += conn.host

        if conn.port:
            uri += ':' + str(int(conn.port))

        if conn.schema:
            uri += '/' + conn.schema

        if conn.extras:
            uri += '?' + conn.extras

        self.mongo_uri = uri

        if conn.password:
            self.shadow_mongo_uri = uri.replace(conn.password, '***')
        else:
            self.shadow_mongo_uri = uri

        self.log.debug(
            'Mongo connection {}: {}'.format(
                mongo_conn_id, self.shadow_mongo_uri))

    def get_conn(self):
        """
        Returns an initialised pymongo MongoClient.
        """
        if self.reuse_connection and self.mongo_client:
            return self.mongo_client

        client = MongoClient(self.mongo_uri)

        self.log.debug(
            'Opening new Mongo connection with {}'.format(
                self.shadow_mongo_uri))

        try:
            client.admin.command('ismaster')
        except ConnectionFailure:
            self.log.error(
                'Mongo connection {} failed: is the database reachable?'.format(
                    self.shadow_mongo_uri))
            raise

        self.mongo_client = client
        return client

    def get_database(self, *args, **kwargs):
        """
        Return database `name`. If not provided, use the default database
        provided in the connection string (or the "schema" field of the
        Airflow connection).

        The full set of supported arguments can be found at:

        https://api.mongodb.com/python/current/api/pymongo/mongo_client.html#pymongo.mongo_client.MongoClient.get_database
        """
        return self.get_conn().get_database(*args, **kwargs)
