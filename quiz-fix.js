/* =====================================================================
   quiz-boost.js — Kết nối nhanh hơn + chịu tải nhiều người hơn
   Cách dùng: XOÁ dòng <script src="quiz-fix.js..."> cũ (bản đó đã được
   gộp sẵn vào index.html và sẽ GHI ĐÈ làm mất tính năng kiểm tra mạng).
   Thay bằng 1 dòng NGAY TRƯỚC </body>, sau thẻ <script> chính:
       <script src="quiz-boost.js?v=1"></script>
   ===================================================================== */
(function () {
  'use strict';

  // 1) Thêm broker thứ 4: mỗi broker chịu ~20 kết nối / IP, thêm 1 broker = thêm ~20 chỗ / wifi
  const EXTRA = 'wss://test.mosquitto.org:8081';
  if (MQTT_BROKERS.indexOf(EXTRA) < 0) MQTT_BROKERS.push(EXTRA);

  // 2) Kết nối nhanh: timeout ngắn, thử lại nhanh, broker từ chối LẦN ĐẦU là đổi broker ngay
  Net.connectOne = function (i) {
    const self = this;
    const c = mqtt.connect(MQTT_BROKERS[i], {
      clientId: 'tpq_' + Math.random().toString(16).slice(2, 10),
      clean: true,
      reconnectPeriod: 1000,
      connectTimeout: 4000,
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
      const rejected = !self.sessionOk || Date.now() - self.sessionAt < 4000;
      if (rejected) self.fails++; else self.fails = 0;
      self.sessionOk = false;
      // Chưa từng vào được: đổi broker ngay lần fail đầu. Đã vào rồi mà rớt: cho 2 lần.
      if (self.fails >= (self.connectedOnce ? 2 : 1)) self.rotate();
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

  // 3) Kiểm tra đường truyền nhanh hơn: gói cách nhau 350ms, đủ gói là xong ngay (~1.5s thay vì ~5s)
  window.runConnTest = function (onDone) {
    const t = connTest = { sent: {}, rtts: [], onPong: null };
    let finished = false, seq = 0, dl = null, iv = null;
    setLoginMsg('🔎 Đang kiểm tra đường truyền (0/' + TEST_TOTAL + ' gói)...', 'warn');
    connProgress(35, 'warn');

    const finish = () => {
      if (finished) return;
      finished = true;
      clearInterval(iv);
      clearTimeout(dl);
      connTest = null;
      const r = t.rtts;
      const avg = r.length ? Math.round(r.reduce((a, b) => a + b, 0) / r.length) : 0;
      const max = r.length ? Math.max.apply(null, r) : 0;
      onDone({ received: r.length, total: TEST_TOTAL, avg: avg, max: max });
    };
    t.onPong = () => { if (t.rtts.length >= TEST_TOTAL) finish(); };

    const sendOne = () => {
      seq++;
      const id = seq;
      t.sent[id] = Date.now();
      Net.pub(NET_UP, { type: 'PING', pid: myPid, seq: id, t: t.sent[id] });
      if (seq >= TEST_TOTAL) {
        clearInterval(iv);
        dl = setTimeout(finish, 2000);
      }
    };
    iv = setInterval(sendOne, 350);
    sendOne();
  };
})();
