import multiprocessing as mp
from threading import Thread, Lock
from collections import deque
import time
from queue import Empty as EmptyError
import json

class MainProcessThread(Thread):
    def __init__(self):
        super(MainProcessThread, self).__init__()
        self.power = True

        self.watch_dog_queue = mp.Queue()
        self.watch_dog_back_queue = mp.Queue()
        self.lock = Lock()

        self.subscribed_now = False  # 현재 구독 되어있는지 여부.
        self.re_connect_pairs = True  # (재)구독 해야할시 참.

        self.list_of_priorities = []  # 이 데이터가 먼저 있어야 웹소켓에 특정 요청 가능. 다른 스레드에서 해당 데이터 획득.
        pass
    def run(self):
        while self.power:
            time.sleep(0.0000000000000000001)

            #  if문 이하는
            #  첫 구독 / 재 구독 / 에러로 인한 재 구독
            #  과정
            if self.re_connect_pairs:  # 맨 처음 구독 또는 미구독 상태 또는 에러 후 재구독 시
                with self.lock:
                    if not self.list_of_priorities:
                        continue
                self.re_connect_pairs = False
                # 이미 구독 되있으면 끊는 과정 필요.
                if self.subscribed_now:  # 기존 구독 있을시.
                    try:
                        self.wm.terminate()  # 구독 제거.
                    except:
                        pass
                    self.wm = None  # 웹소켓 매니저 None
                    self.subscribed_now = False  # 기존 구독 없음.
                try:
                    with self.lock:
                        self.watch_dog_back_queue = mp.Queue()  # 갱신 안하면 에러.
                        self.watch_dog_queue = mp.Queue()  # 갱신 안하면 에러.
                        self.wm = WebSocketManager("trade", self.list_of_priorities, watch_dog_queue=self.watch_dog_queue, watch_dog_back_queue=self.watch_dog_back_queue)
                    self.subscribed_now = True  # 구독 완료
                except Exception as e:
                    error_wait = 10  # 구독 에러로 error_wait 초 대기후 재구독 시도
                    st = time.time()
                    while self.power:
                        tn = time.time()
                        if tn - st > error_wait:
                            break
                        time.sleep(0.1)
                    self.re_connect_pairs = True  # 재구독 플래그 참으로.
                    self.subscribed_now = False  # 구독 되어있는것 취소.
                    continue

            if self.power:  # 단순히 살아있는지 여부. 추후 process terminate시 wsManager 재실행되어 출동 가능성 때문
                self.wm.get()  # wm 프로세스 시작(사실상 start)
                self.watch_dog_queue.put_nowait(True)  # 데이터 달라고 요청

            try:
                queue_recieved = self.watch_dog_back_queue.get(timeout=0.5)  # 데이터 수신
            except EmptyError:  # 큐 비었을시 continue.
                print('continued')
                continue

            # 에러 날아왔는지 판별
            if isinstance(queue_recieved, str):
                if queue_recieved == 'ConnectionClosedError':
                    print(f'##########\n웹소켓 수신 에러로 재구독 실행:\n{e}\n##########')
                    self.re_connect_pairs = True
                    continue

            data_list = list(queue_recieved)  # deque 형태로 데이터 받음.
            for _data in data_list:
                data = json.loads(_data)  # 기존 get()을 통해 받던 형태.


class WebSocketManager(mp.Process):
    def __init__(self, type: str, codes: list, qsize: int = 1000, watch_dog_queue=None, watch_dog_back_queue=None):
        self.alive = False
        self.power = True
        self.watch_dog_back_queue = watch_dog_back_queue  # 정보 발송 용도
        self.watch_dog_queue = watch_dog_queue  # 정보 발송 여부 수신 용도
        self.deque = deque([], maxlen=5000)  # 현재 프로세스에서 수신 데이터 저장
        self.lock = Lock()  # 스레드 데이터 간섭 방지용
        # self.__q = mp.Queue(qsize)  # 원 코드. 기존 get 에서 바로 쓰는 형태
        super().__init__()

    async def __connect_socket(self):
        uri = ""
        async for websocket in websockets.connect(uri, ping_interval=60):
            try:
                data = ''
                await websocket.send(json.dumps(data))
                while self.alive:
                    recv_data = await websocket.recv()
                    # recv_data = recv_data.decode('utf8')  # 원 코드 A. 다른 프로세스에서 디코딩 시 효율 더 높음
                    # self.__q.put(json.loads(recv_data))  # 원 코드 B. put 후 대기때문에 데이터 수신 지연 발생.
                    self.deque.append(recv_data)  # B의 개선 코드. 디큐에 데이터 저장 후 요청시 리스트로 한번에 보내 처리하는 형태
            except websockets.ConnectionClosed:
                self.watch_dog_back_queue.put('ConnectionClosedError')  # 에러도 기존 self.__q가 아닌 self.watch_dog_back_queue를 이용
                continue
    def run(self):
        self.watch_dog_thread: Thread = Thread(target=self.request_watch_dog, args=())  # 타 프로세스에서의 요청 감지용
        self.watch_dog_thread.start()  # 이벤트 리슨용
        asyncio.run(self.__connect_socket())

    def get(self):
        if self.alive is False:
            self.alive = True
            self.start()
        # return self.__q.get()  # 원 코드. 수정한 코드에서 self.get()은 단순 웹소켓 연결의 의미
        return

    def request_watch_dog(self):
        while self.power:  # alive로 작성시 self.get()와 간섭
            time.sleep(0.000000000001)  # 스레드간 전환시간용
            watch = self.watch_dog_queue.get()  #
            if watch:
                with self.lock:
                    deque_copied = list(self.deque.copy())  # 디큐 째로 복사하고
                    self.deque = deque([], maxlen=5000)  # 초기화
                self.watch_dog_back_queue.put_nowait(deque_copied)  # 데이터 전달

    def terminate(self):
        self.alive = False
        self.power = False
        super().terminate()