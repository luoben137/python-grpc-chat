from concurrent import futures

import grpc
import queue
import time

import proto.startx_service_pb2 as startx_service
import proto.startx_service_pb2_grpc as rpc


class StartxServer(rpc.StartxServerServicer):  # inheriting here from the protobuf rpc file which is generated
    def __init__(self):
        self.connections = {}
        self.message_queues = {}  # 新增用于存储每个用户的消息队列

    # The stream which will be used to send new messages to clients
    def MessageStream(self, req, context):
        """
        This is a response-stream type call. This means the server can keep sending messages
        Every client opens this connection and waits for server to send new messages

        :param request_iterator:
        :param context:
        :return:
        """
        username = req.name
        q = self.message_queues.get(username)  # 获取用户的消息队列

        try:
            while True:
                item = q.get()
                yield item
        except GeneratorExit:
            # 当流结束时，清理资源
            self.handle_client_disconnect(username)

    def Register(self, request: startx_service.User, context):
        print(f"[REGISTER] {request.name}")
        self.connections[request.name] = context
        self.message_queues[request.name] = queue.Queue()
        return startx_service.Empty()

    def handle_client_disconnect(self, username):
        # 移除断开的连接和对应的消息队列
        if username in self.connections:
            del self.connections[username]
        if username in self.message_queues:
            del self.message_queues[username]
        print(f"[DISCONNECT] {username}")

    def SendNote(self, request: startx_service.Note, context):
        print(f"[SEND] {request.name} to {request.message.split(' ')[0]}")
        target_username = request.message.split(' ')[0]
        if target_username in self.connections:
            target_queue = self.message_queues[target_username]
            target_queue.put(request)  # 将消息添加到目标用户的队列中
            print(f"Queued note to {target_username}: {request.message}")
        else:
            print(f"User {target_username} not connected.")
        return startx_service.Empty()


if __name__ == '__main__':
    port = 50051  # a random port for the server to run on
    # the workers is like the amount of threads that can be opened at the same time, when there are 10 clients connected
    # then no more clients able to connect to the server.
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))  # create a gRPC server
    rpc.add_StartxServerServicer_to_server(StartxServer(), server)  # register the server to gRPC
    # gRPC basically manages all the threading and server responding logic, which is perfect!
    print('Starting server. Listening...')
    server.add_insecure_port('[::]:' + str(port))
    server.start()
    # Server starts in background (in another thread) so keep waiting
    # if we don't wait here the main thread will end, which will end all the child threads, and thus the threads
    # from the server won't continue to work and stop the server
    while True:
        time.sleep(64 * 64 * 100)
