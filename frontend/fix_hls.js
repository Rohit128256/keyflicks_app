const fs = require('fs');
const content = fs.readFileSync('c:/Users/rohit/Desktop/latest keyflicks/keyflicks_app/frontend/src/components/VideoPlayer.jsx', 'utf8');

let newContent = content.replace(/import videojs from 'video\.js';\r?\nimport 'video\.js\/dist\/video-js\.css';\r?\nimport 'videojs-contrib-quality-levels';\r?\n/, "import Hls from 'hls.js';\n");

newContent = newContent.replace('const videoWrapRef = useRef(null);', 'const videoRef = useRef(null);\n  const hlsRef = useRef(null);');
newContent = newContent.replace('const playerRef = useRef(null);\n', '');

// Delete Video.js initialization entirely
const initRegex = /\/\/ ── Video\.js init ──.*?(?=  \/\/ ── Keyboard shortcuts)/s;

const hlsInit = `// ── Hls.js init ─────────────────────────────────────────────────────────
  useEffect(() => {
    if (!videoRef.current || !videoId) return;

    const video = videoRef.current;
    let hls;

    const getToken = () => useAuthStore.getState().accessToken;

    if (Hls.isSupported()) {
      hls = new Hls({
        xhrSetup: (xhr, url) => {
          xhr.setRequestHeader('Authorization', \`Bearer \${getToken()}\`);
        },
      });
      hlsRef.current = hls;

      hls.loadSource(\`/api/master/\${videoId}\`);
      hls.attachMedia(video);

      hls.on(Hls.Events.MANIFEST_PARSED, (event, data) => {
        const heights = [...new Set(data.levels.map(l => l.height).filter(Boolean))];
        setQualities(heights.sort((a, b) => b - a));
      });

      hls.on(Hls.Events.ERROR, async (event, data) => {
        if (data.fatal) {
          switch (data.type) {
            case Hls.ErrorTypes.NETWORK_ERROR:
              if (data.response && (data.response.code === 403 || data.response.code === 401)) {
                try {
                  const res = await api.get('/refresh-token');
                  if (res.data.access_token) {
                    useAuthStore.getState().setAccessToken(res.data.access_token);
                    hls.loadSource(\`/api/master/\${videoId}?t=\${Date.now()}\`);
                    hls.attachMedia(video);
                  }
                } catch (e) {
                   console.error("Token refresh failed");
                }
              } else {
                hls.startLoad();
              }
              break;
            case Hls.ErrorTypes.MEDIA_ERROR:
              hls.recoverMediaError();
              break;
            default:
              hls.destroy();
              break;
          }
        }
      });
    } else if (video.canPlayType('application/vnd.apple.mpegurl')) {
      video.src = \`/api/master/\${videoId}\`;
    }

    // ── Sync events ──────────────────────────────────
    const onPlay = () => { setPlaying(true); playingRef.current = true; revealControls(); };
    const onPause = () => { setPlaying(false); playingRef.current = false; setCtrlVisible(true); clearTimeout(hideTimer.current); };
    const onEnded = () => { setPlaying(false); playingRef.current = false; setCtrlVisible(true); };
    const onTimeUpdate = () => setCT(video.currentTime);
    const onDurationChange = () => setDur(video.duration || 0);
    const onVolumeChange = () => { setVol(video.volume); setMuted(video.muted); };
    let waitTimeout;
    const onWait = () => { clearTimeout(waitTimeout); waitTimeout = setTimeout(() => setLoading(true), 300); };
    const clearWait = () => { clearTimeout(waitTimeout); setLoading(false); };
    const onProgress = () => {
      if (video.buffered.length > 0) {
        setBuf(video.buffered.end(video.buffered.length - 1));
      }
    };

    video.addEventListener('play', onPlay);
    video.addEventListener('pause', onPause);
    video.addEventListener('ended', onEnded);
    video.addEventListener('timeupdate', onTimeUpdate);
    video.addEventListener('durationchange', onDurationChange);
    video.addEventListener('volumechange', onVolumeChange);
    video.addEventListener('waiting', onWait);
    video.addEventListener('seeking', onWait);
    video.addEventListener('playing', clearWait);
    video.addEventListener('canplay', clearWait);
    video.addEventListener('seeked', clearWait);
    video.addEventListener('progress', onProgress);

    return () => {
      clearTimeout(waitTimeout);
      clearTimeout(hideTimer.current);
      if (hlsRef.current) {
        hlsRef.current.destroy();
        hlsRef.current = null;
      }
      video.removeEventListener('play', onPlay);
      video.removeEventListener('pause', onPause);
      video.removeEventListener('ended', onEnded);
      video.removeEventListener('timeupdate', onTimeUpdate);
      video.removeEventListener('durationchange', onDurationChange);
      video.removeEventListener('volumechange', onVolumeChange);
      video.removeEventListener('waiting', onWait);
      video.removeEventListener('seeking', onWait);
      video.removeEventListener('playing', clearWait);
      video.removeEventListener('canplay', clearWait);
      video.removeEventListener('seeked', clearWait);
      video.removeEventListener('progress', onProgress);
    };
  }, [videoId, revealControls]);
`;

newContent = newContent.replace(initRegex, hlsInit + '\n\n  // ── Keyboard shortcuts');

// Replace player.something with video.something
newContent = newContent.replace(/playerRef\.current\.currentTime\((.*?)\)/g, 'videoRef.current.currentTime = $1');
newContent = newContent.replace(/playerRef\.current\.volume\((.*?)\)/g, 'videoRef.current.volume = $1');
newContent = newContent.replace(/playerRef\.current\.muted\((.*?)\)/g, 'videoRef.current.muted = $1');
newContent = newContent.replace(/playerRef\.current\.pause\(\)/g, 'videoRef.current.pause()');
newContent = newContent.replace(/playerRef\.current\.play\(\)/g, 'videoRef.current.play()');
newContent = newContent.replace(/playerRef\.current\.playbackRate\((.*?)\)/g, 'videoRef.current.playbackRate = $1');

newContent = newContent.replace(/const handleQuality.*?};/s, `const handleQuality = (q) => {
    setQuality(q);
    setShowQuality(false);
    if (!hlsRef.current) return;
    if (q === 'auto') {
      hlsRef.current.currentLevel = -1;
    } else {
      const idx = hlsRef.current.levels.findIndex(l => l.height === q);
      if (idx !== -1) hlsRef.current.currentLevel = idx;
    }
  };`);

newContent = newContent.replace(/<div data-vjs-player.*?ref=\{videoWrapRef\}.*?\/>/g, '<video ref={videoRef} className="w-full h-full object-contain" playsInline />');

// Remove video.js CSS block
const cssRegex = /\/\* Hide ALL default Video\.js UI.*?visibility: hidden !important;\s*\}/s;
newContent = newContent.replace(cssRegex, '');

fs.writeFileSync('c:/Users/rohit/Desktop/latest keyflicks/keyflicks_app/frontend/src/components/VideoPlayer.jsx', newContent);
console.log('done replacing');
