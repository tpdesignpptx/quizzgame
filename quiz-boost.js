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

  // =====================================================================
  // 4) TẮT MÀN HÌNH / CHUYỂN APP RỒI MỞ LẠI => TỰ KẾT NỐI LẠI, KHÔNG PHẢI ĐĂNG NHẬP LẠI
  // =====================================================================
  const SESS_KEY = 'tpq_sess';
  function saved() {
    try {
      const s = JSON.parse(sessionStorage.getItem(SESS_KEY) || 'null');
      return s && s.code && s.name ? s : null;
    } catch (e) { return null; }
  }
  function clearSaved() { try { sessionStorage.removeItem(SESS_KEY); } catch (e) {} }

  // --- 4a) Giữ màn hình luôn sáng trong lúc chơi (Wake Lock) ---
  let wl = null;
  async function acquireWake() {
    try {
      if ('wakeLock' in navigator && !document.hidden && !wl) {
        wl = await navigator.wakeLock.request('screen');
        wl.addEventListener('release', () => { wl = null; });
      }
    } catch (e) { wl = null; }
  }
  function releaseWake() { try { if (wl) wl.release(); } catch (e) {} wl = null; }

  // --- 4b) Banner "đang kết nối lại" ---
  function banner(show, text) {
    let b = document.getElementById('resume-banner');
    if (!b) {
      b = document.createElement('div');
      b.id = 'resume-banner';
      b.style.cssText = 'position:fixed;top:0;left:0;right:0;z-index:9999;padding:10px 14px;text-align:center;font-weight:800;font-size:14px;background:#d97706;color:#fff;box-shadow:0 4px 20px rgba(0,0,0,.5)';
      document.body.appendChild(b);
    }
    b.textContent = text || '';
    b.style.display = show ? 'block' : 'none';
  }

  // --- 4c) Lưu thông tin phòng khi học sinh bấm vào ---
  const origJoin = window.playerJoin;
  window.playerJoin = function () {
    const code = document.getElementById('roomCodeInput').value.trim();
    const name = document.getElementById('playerName').value.trim();
    if (code && name) {
      try { sessionStorage.setItem(SESS_KEY, JSON.stringify({ code: code, name: name, t: Date.now() })); } catch (e) {}
      acquireWake();
    }
    return origJoin.apply(this, arguments);
  };

  // --- 4d) Vào phòng xong: xin phòng gửi trạng thái câu hỏi hiện tại ngay + giữ sáng màn hình ---
  const origStartWatch = window.startNetWatch;
  window.startNetWatch = function () {
    origStartWatch.apply(this, arguments);
    Net.pub(NET_UP, { type: 'HELLO', pid: myPid });
    acquireWake();
  };

  // --- 4e) Kết thúc game / bị loại thì xoá phiên đã lưu ---
  const origHandle = window.handlePlayerMessage;
  window.handlePlayerMessage = function (data) {
    if (data && (data.type === 'END_GAME' || (data.type === 'KICKED' && data.pid === myPid))) {
      clearSaved();
      releaseWake();
    }
    return origHandle.apply(this, arguments);
  };

  // --- 4f) Tự kết nối lại âm thầm (giữ nguyên màn hình đang chơi) ---
  let resuming = false, resumeTimer = null;
  const origForce = window.forceRelogin;

  function startResume(reason) {
    const s = saved();
    if (!s) return origForce(reason || 'Mất kết nối.');
    if (resuming) return;
    resuming = true;

    stopNetWatch();
    clearJoinTimers();
    Net.close();
    joinedOk = false;
    banner(true, '🔄 Đang kết nối lại phòng chơi...');

    myPid = getPid(s.code);
    myPlayerName = s.name;
    setNetTopics(s.code);

    let retry = null;
    const sendJoin = () => Net.pub(NET_UP, { type: 'PLAYER_JOIN', pid: myPid, name: s.name, rtt: myRtt });
    const giveUp = (txt) => {
      clearInterval(retry);
      clearTimeout(resumeTimer);
      resuming = false;
      banner(false);
      Net.close();
      origForce((txt || reason || 'Mất kết nối.') + ' Không kết nối lại được.');
    };
    resumeTimer = setTimeout(() => giveUp(), 45000);

    Net.init([NET_DOWN, ackTopic(myPid)], (data) => {
      if (data.type === 'PONG') return;
      if (data.type === 'JOIN_FULL' && data.pid === myPid && !joinedOk) { giveUp('Phòng đã đầy.'); return; }
      if (data.type === 'JOIN_ACK') {
        if (data.pid === myPid && !joinedOk) {
          joinedOk = true;
          clearInterval(retry);
          clearTimeout(resumeTimer);
          resuming = false;
          banner(false);
          startNetWatch();   // đã bọc ở trên: tự gửi HELLO để nhận lại câu hỏi hiện tại
        }
        return;
      }
      if (!joinedOk) return;
      lastAdminMsgAt = Date.now();
      if (data.type === 'BEAT') {
        if (data.ended && playerPlaying) handlePlayerMessage({ type: 'END_GAME' });
        return;
      }
      handlePlayerMessage(data);
    }, () => {
      sendJoin();
      retry = setInterval(() => { if (Net.isConnected() && !joinedOk) sendJoin(); }, 1500);
    }, true);
  }

  // Mọi chỗ trong code cũ đang gọi "đăng nhập lại" => đổi thành "kết nối lại âm thầm"
  window.forceRelogin = function (reason) {
    if (joinedOk && saved()) startResume(reason);
    else origForce(reason);
  };

  // Mở lại màn hình / quay lại app / có mạng lại: nếu đã rời > 2 giây thì nối lại luôn (socket thường đã chết)
  // Nghe nhiều loại sự kiện vì mỗi hãng điện thoại / trình duyệt báo một kiểu
  let hiddenAt = 0, lastWake = 0;
  function onWake() {
    const now = Date.now();
    if (now - lastWake < 1500) return;      // gộp các sự kiện đến cùng lúc
    lastWake = now;
    if (!saved()) return;
    acquireWake();
    const away = hiddenAt ? now - hiddenAt : 0;
    hiddenAt = 0;
    if (joinedOk && !resuming && (away > 2000 || !Net.isConnected())) startResume('');
  }
  document.addEventListener('visibilitychange', () => { if (document.hidden) hiddenAt = Date.now(); else onWake(); });
  window.addEventListener('pageshow', onWake);
  window.addEventListener('focus', onWake);
  window.addEventListener('online', onWake);
  document.addEventListener('resume', onWake);

  // Giám sát: đang chơi mà > 8 giây không nhận được tín hiệu nào từ phòng (phòng gửi mỗi 3 giây)
  // => kết nối đã chết ngầm, tự nối lại ngay thay vì chờ 15 giây rồi bắt đăng nhập lại
  setInterval(() => {
    if (!joinedOk || resuming || document.hidden || !saved()) return;
    if (Date.now() - lastAdminMsgAt > 8000) startResume('');
  }, 1500);

  console.log('quiz-boost v4 loaded');

  // --- 4g) Tải lại trang (trình duyệt xoá tab khi tắt màn hình) => tự vào lại phòng ---
  window.addEventListener('load', () => {
    const s = saved();
    if (!s || Date.now() - s.t > 3 * 3600 * 1000) return;
    const urlRoom = new URLSearchParams(location.search).get('room');
    if (urlRoom && urlRoom !== s.code) return;
    switchRole('player');
    document.getElementById('roomCodeInput').value = s.code;
    document.getElementById('playerName').value = s.name;
    window.playerJoin();
  });

  // =====================================================================
  // 5) PHÍA QUẢN TRỊ: học sinh gửi HELLO => gửi ngay trạng thái hiện tại (không chờ 3 giây)
  // =====================================================================
  const origAdminMsg = window.handleAdminMessage;
  window.handleAdminMessage = function (data) {
    if (data && data.type === 'HELLO' && typeof data.pid === 'string' && /^p[a-z0-9]{4,12}$/.test(data.pid)) {
      if (players[data.pid] && gameRunning && roomReady && adminPhase !== 'idle') {
        const msg = {
          type: 'SYNC',
          qIndex: currentQIndex,
          total: questions.length,
          q: publicQuestion(currentQIndex),
          phase: adminPhase,
          remainingMs: Math.max(0, adminPhaseEnd - Date.now()),
          timeLimit: timeLimit
        };
        if (adminPhase === 'reveal' && adminLastReveal) {
          msg.correctOption = adminLastReveal.correctOption;
          msg.scores = adminLastReveal.scores;
        }
        Net.pub(NET_DOWN, msg);
      }
      return;
    }
    return origAdminMsg.apply(this, arguments);
  };
})();
