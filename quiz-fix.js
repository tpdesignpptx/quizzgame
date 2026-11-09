/* =====================================================================
   quiz-fix.js  —  VÁ LỖI "39 HỌC SINH VÀO NHƯNG CHỈ 19 CHƠI ĐƯỢC"
   Cách dùng: để file này cạnh index.html, rồi thêm 1 dòng NGAY TRƯỚC </body>
   (nằm SAU thẻ <script> chính):
       <script src="quiz-fix.js?v=2"></script>

   Nguyên nhân: mỗi học sinh mở 3 kết nối (3 broker) cùng lúc. Cả lớp dùng
   chung 1 wifi = chung 1 IP, nên broker công cộng chặn khi vượt ~20 kết nối
   / IP / broker. Bản vá:
   1) Quản trị: vẫn nối cả 3 broker (nghe + phát cả 3).
   2) Học sinh: chỉ nối 1 broker (chọn ngẫu nhiên); bị từ chối / rớt thì
      tự chuyển sang broker kế tiếp -> tải tự cân bằng giữa 3 broker.
   3) Gửi đáp án lặp lại vài lần (QoS 0 có thể rớt gói); admin đã tự bỏ
      qua bản trùng nên không bị cộng điểm 2 lần.
   ===================================================================== */
(function () {
  'use strict';

  // ---------- Net: ghi đè các hàm kết nối ----------
  Net.single = false;
  Net.idx = 0;
  Net.fails = 0;
  Net.sessionOk = false;
  Net.sessionAt = 0;
  Net.subTopics = [];
  Net.onMsg = null;
  Net.onFirst = null;

  Net.init = function (subTopic, onMsg, onFirstConnect, single) {
    this.close();
    this.subTopics = Array.isArray(subTopic) ? subTopic : [subTopic];
    this.onMsg = onMsg;
    this.onFirst = onFirstConnect;
    this.single = !!single;
    this.fails = 0;
    this.sessionOk = false;
    if (this.single) {
      // Học sinh: 1 kết nối duy nhất, chọn broker ngẫu nhiên
      this.idx = Math.floor(Math.random() * MQTT_BROKERS.length);
      this.connectOne(this.idx);
    } else {
      // Quản trị: nối tất cả broker
      MQTT_BROKERS.forEach((u, i) => this.connectOne(i));
    }
  };

  Net.connectOne = function (i) {
    const self = this;
    const c = mqtt.connect(MQTT_BROKERS[i], {
      clientId: 'tpq_' + Math.random().toString(16).slice(2, 10),
      clean: true,
      reconnectPeriod: 2000,
      connectTimeout: 6000,
      keepalive: 30
    });

    c.on('connect', () => {
      if (self.clients.indexOf(c) < 0) return;
      self.sessionOk = true;
      self.sessionAt = Date.now();
      c.subscribe(self.subTopics, { qos: 0 }, (err) => {
        if (err) return;
        if (!self.connectedOnce) {
          self.connectedOnce = true;
          if (self.onFirst) self.onFirst();
        }
      });
    });

    c.on('close', () => {
      if (!self.single || self.clients.indexOf(c) < 0) return;
      // Kết nối chưa từng thành công hoặc sống < 4 giây = bị broker từ chối
      if (!self.sessionOk || Date.now() - self.sessionAt < 4000) self.fails++;
      else self.fails = 0;
      self.sessionOk = false;
      if (self.fails >= 2) self.rotate();
    });

    c.on('message', (topic, payload) => {
      try {
        const m = JSON.parse(payload.toString());
        if (m._id) {
          if (self.seen.has(m._id)) return;
          self.seen.add(m._id);
          if (self.seen.size > 500) self.seen.delete(self.seen.values().next().value);
        }
        if (self.onMsg) self.onMsg(m);
      } catch (e) {}
    });

    c.on('error', () => {});
    this.clients.push(c);
  };

  // Chuyển sang broker kế tiếp (chỉ dùng cho học sinh)
  Net.rotate = function () {
    const old = this.clients;
    this.clients = [];
    this.fails = 0;
    this.sessionOk = false;
    old.forEach(c => { try { c.end(true); } catch (e) {} });
    this.idx = (this.idx + 1) % MQTT_BROKERS.length;
    this.connectOne(this.idx);
  };

  Net.close = function () {
    const old = this.clients;
    this.clients = [];
    this.connectedOnce = false;
    this.fails = 0;
    this.sessionOk = false;
    this.seen.clear();
    old.forEach(c => { try { c.end(true); } catch (e) {} });
  };

  // ---------- Học sinh vào phòng ----------
  let joining = false;
  window.playerJoin = function () {
    if (joining) return;
    const code = document.getElementById('roomCodeInput').value.trim();
    myPlayerName = document.getElementById('playerName').value.trim();
    const msg = document.getElementById('player-login-msg');

    if (!code) return alert('Vui lòng nhập Mã Phòng!');
    if (!myPlayerName) return alert('Vui lòng nhập Tên!');

    joining = true;
    msg.innerText = '⏳ Đang kết nối tới phòng...';

    myPid = 'p' + Math.random().toString(36).slice(2, 9);
    setNetTopics(code);

    let joined = false;
    const sendJoin = () => Net.pub(NET_UP, { type: 'PLAYER_JOIN', pid: myPid, name: myPlayerName });

    const joinRetry = setInterval(() => { if (Net.isConnected() && !joined) sendJoin(); }, 2000);
    const joinTimeout = setTimeout(() => {
      if (joined) return;
      clearInterval(joinRetry);
      msg.innerText = Net.isConnected()
        ? '❌ Không tìm thấy phòng này. Kiểm tra lại mã phòng và chắc chắn máy chiếu đã hiện mã!'
        : '❌ Không kết nối được tới máy chủ trung gian. Hãy thử đổi wifi/4G rồi vào lại!';
      Net.close();
      joining = false;
    }, 45000);

    Net.init([NET_DOWN, ackTopic(myPid)], (data) => {
      if (data.type === 'JOIN_FULL' && data.pid === myPid && !joined) {
        clearInterval(joinRetry);
        clearTimeout(joinTimeout);
        msg.innerText = '❌ Phòng đã đầy, không thể tham gia thêm!';
        Net.close();
        joining = false;
        return;
      }
      if (data.type === 'JOIN_ACK') {
        if (data.pid === myPid && !joined) {
          joined = true;
          clearInterval(joinRetry);
          clearTimeout(joinTimeout);
          document.getElementById('player-login').classList.add('hidden');
          document.getElementById('player-wait').classList.remove('hidden');
          document.getElementById('player-display-name').innerText = myPlayerName;
        }
        return;
      }
      if (joined) handlePlayerMessage(data);
    }, sendJoin, true);
  };

  // ---------- Học sinh chọn đáp án (gửi lặp lại cho chắc) ----------
  window.playerSelectOption = function (opt) {
    if (playerHasAnswered) return;
    playerHasAnswered = true;
    mySelectedOption = opt;
    disablePlayerButtons();

    document.getElementById('player-options-box').classList.add('hidden');
    document.getElementById('player-answered-box').classList.remove('hidden');
    document.getElementById('player-chosen-text').innerText = `✅ Đã chọn đáp án: ${opt}`;
    document.getElementById('player-status-msg').innerText = '';

    const qi = currentQIndex;
    const send = () => Net.pub(NET_UP, { type: 'SUBMIT_ANSWER', pid: myPid, qIndex: qi, selected: opt });
    send();
    let n = 0;
    const iv = setInterval(() => {
      if (playerPhase === 'reveal' || playerPhaseQ !== qi || ++n > 4) { clearInterval(iv); return; }
      send();
    }, 1200);
  };
})();
