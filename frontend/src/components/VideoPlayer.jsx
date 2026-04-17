'use client';
import { useEffect, useRef, useState, useCallback } from 'react';
import Hls from 'hls.js';
import { useAuthStore } from '@/lib/store';
import { api } from '@/lib/api';
import {
  Play, Pause,
  Volume2, Volume1, VolumeX,
  Maximize, Minimize,
  RotateCcw, RotateCw,
  Settings, Gauge,
} from 'lucide-react';

// ─── Helpers ───────────────────────────────────────────────────────────────
function fmt(sec) {
  if (!sec || isNaN(sec) || !isFinite(sec)) return '0:00';
  const h = Math.floor(sec / 3600);
  const m = Math.floor((sec % 3600) / 60);
  const s = Math.floor(sec % 60);
  if (h > 0) return `${h}:${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`;
  return `${m}:${String(s).padStart(2, '0')}`;
}

const SPEEDS = [0.25, 0.5, 0.75, 1, 1.25, 1.5, 2];

// ─── Component ─────────────────────────────────────────────────────────────
export default function VideoPlayer({ videoId }) {
  const videoRef = useRef(null);
  const hlsRef = useRef(null);
    const containerRef = useRef(null);
  const progressRef = useRef(null);
  const hideTimer = useRef(null);
  const playingRef = useRef(false); // sync ref for callbacks that capture stale state
  const clickTimer = useRef(null);  // debounce single vs double click

  // ── Playback state ────────────────────────────────────────────────────────
  const [playing, setPlaying] = useState(false);
  const [currentTime, setCT] = useState(0);
  const [duration, setDur] = useState(0);
  const [buffered, setBuf] = useState(0);
  const [volume, setVol] = useState(1);
  const [muted, setMuted] = useState(false);
  const [loading, setLoading] = useState(true);

  // ── Controls / UI state ───────────────────────────────────────────────────
  const [ctrlVisible, setCtrlVisible] = useState(true);
  const [fullscreen, setFullscreen] = useState(false);
  const [qualities, setQualities] = useState([]);   // sorted heights
  const [quality, setQuality] = useState('auto');
  const [rate, setRate] = useState(1);
  const [showRate, setShowRate] = useState(false);
  const [showQuality, setShowQuality] = useState(false);
  const [flash, setFlash] = useState(null); // { text, key }
  const [actionFlash, setActionFlash] = useState(null); // { type, key }
  const [tip, setTip] = useState(null); // { pct, time } for progress tooltip
  const [dragging, setDragging] = useState(false);

  // Controls are visible when explicitly shown OR when video is paused
  const ctrlShown = ctrlVisible || !playingRef.current;

  // ── Flash helper ──────────────────────────────────────────────────────────
  const doFlash = useCallback((text) => {
    setFlash({ text, key: Date.now() });
    setTimeout(() => setFlash(null), 650);
  }, []);

  const doActionFlash = useCallback((type) => {
    setActionFlash({ type, key: Date.now() });
    setTimeout(() => setActionFlash(null), 850);
  }, []);

  // ── Controls auto-hide ────────────────────────────────────────────────────
  const revealControls = useCallback(() => {
    setCtrlVisible(true);
    clearTimeout(hideTimer.current);
    if (playingRef.current) {
      hideTimer.current = setTimeout(() => setCtrlVisible(false), 3000);
    }
  }, []);

  // ── Hls.js init ─────────────────────────────────────────────────────────
  useEffect(() => {
    if (!videoRef.current || !videoId) return;

    const video = videoRef.current;
    let hls;

    const getToken = () => useAuthStore.getState().accessToken;

    if (Hls.isSupported()) {
      hls = new Hls({
        xhrSetup: (xhr, url) => {
          xhr.setRequestHeader('Authorization', `Bearer ${getToken()}`);
        },
      });
      hlsRef.current = hls;

      hls.loadSource(`/api/master/${videoId}`);
      hls.attachMedia(video);

      hls.on(Hls.Events.MANIFEST_PARSED, (event, data) => {
        const heights = [...new Set(data.levels.map(l => l.height).filter(Boolean))];
        setQualities(heights.sort((a, b) => b - a));
        // Attempt Autoplay
        video.play().catch(() => {});
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
                    hls.loadSource(`/api/master/${videoId}?t=${Date.now()}`);
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
      video.src = `/api/master/${videoId}`;
      video.play().catch(() => {});
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


  // ── Keyboard shortcuts ────────────────────────────────────────────────────
  useEffect(() => {
    const onKey = (e) => {
      const v = videoRef.current;
      if (!v) return;
      const tag = document.activeElement?.tagName;
      if (['INPUT', 'TEXTAREA', 'SELECT'].includes(tag)) return;

      switch (e.key) {
        case ' ': case 'k': case 'K':
          e.preventDefault();
          togglePlay();
          revealControls();
          break;
        case 'ArrowLeft': case 'j': case 'J':
          e.preventDefault();
          v.currentTime = Math.max(0, v.currentTime - 10);
          setCT(v.currentTime);
          doFlash('−10s'); revealControls();
          break;
        case 'ArrowRight': case 'l': case 'L':
          e.preventDefault();
          v.currentTime = Math.min(v.duration || 0, v.currentTime + 10);
          setCT(v.currentTime);
          doFlash('+10s'); revealControls();
          break;
        case 'ArrowUp':
          e.preventDefault();
          v.volume = Math.min(1, v.volume + 0.1); revealControls();
          break;
        case 'ArrowDown':
          e.preventDefault();
          v.volume = Math.max(0, v.volume - 0.1); revealControls();
          break;
        case 'm': case 'M':
          e.preventDefault();
          v.muted = !v.muted; revealControls();
          break;
        case 'f': case 'F':
          e.preventDefault();
          toggleFS();
          break;
        default:
          if ('0123456789'.includes(e.key)) {
            e.preventDefault();
            v.currentTime = (v.duration || 0) * parseInt(e.key) / 10;
            revealControls();
          }
      }
    };
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
  }, [doFlash, revealControls]); // toggleFS removed from array to prevent cyclical re-renders

  const toggleFS = () => {
    if (!document.fullscreenElement && !document.webkitFullscreenElement) {
      if (containerRef.current?.requestFullscreen) containerRef.current.requestFullscreen();
      else if (containerRef.current?.webkitRequestFullscreen) containerRef.current.webkitRequestFullscreen();
    } else {
      if (document.exitFullscreen) document.exitFullscreen();
      else if (document.webkitExitFullscreen) document.webkitExitFullscreen();
    }
  };

  useEffect(() => {
    const onFS = () => setFullscreen(!!(document.fullscreenElement || document.webkitFullscreenElement));
    document.addEventListener('fullscreenchange', onFS);
    document.addEventListener('webkitfullscreenchange', onFS);
    return () => {
      document.removeEventListener('fullscreenchange', onFS);
      document.removeEventListener('webkitfullscreenchange', onFS);
    };
  }, []);

  // ── Control actions ───────────────────────────────────────────────────────
  const togglePlay = () => {
    const v = videoRef.current;
    if (!v) return;
    if (v.paused) {
      v.play().catch(() => {});
      doActionFlash('play');
    } else {
      v.pause();
      doActionFlash('pause');
    }
  };

  const changeQuality = (q) => {
    setQuality(q);
    setShowQuality(false);
    if (!hlsRef.current) return;
    if (q === 'auto') {
      hlsRef.current.nextLevel = -1;
    } else {
      const idx = hlsRef.current.levels.findIndex(l => l.height === q);
      if (idx !== -1) hlsRef.current.nextLevel = idx;
    }
  };

  const changeRate = (r) => {
    const v = videoRef.current;
    if (v) v.playbackRate = r;
    setRate(r);
    setShowRate(false);
  };

  const toggleMute = () => {
    const v = videoRef.current;
    if (v) v.muted = !v.muted;
  };

  const changeVolume = (val) => {
    const v = videoRef.current;
    if (!v) return;
    v.volume = val;
    if (val === 0) v.muted = true;
    else if (v.muted) v.muted = false;
  };

  const seekTo = (clientX) => {
    if (!progressRef.current || !videoRef.current) return;
    const rect = progressRef.current.getBoundingClientRect();
    let p = (clientX - rect.left) / rect.width;
    if (p < 0) p = 0; if (p > 1) p = 1;
    videoRef.current.currentTime = p * duration;
    setCT(p * duration);
  };

  useEffect(() => {
    if (!dragging) return;
    const onMove = (e) => seekTo(e.clientX);
    const onUp = () => setDragging(false);
    document.addEventListener('mousemove', onMove);
    document.addEventListener('mouseup', onUp);
    return () => {
      document.removeEventListener('mousemove', onMove);
      document.removeEventListener('mouseup', onUp);
    };
  }, [dragging, duration]);

  // ── Derived values ────────────────────────────────────────────────────────
  const pct = duration > 0 ? (currentTime / duration) * 100 : 0;
  const bufPct = duration > 0 ? (buffered / duration) * 100 : 0;
  const VolumeIcon = (muted || volume === 0) ? VolumeX : volume < 0.5 ? Volume1 : Volume2;

  // ── Render ────────────────────────────────────────────────────────────────
  return (
    <div
      ref={containerRef}
      className="relative w-full bg-black overflow-hidden rounded-xl"
      style={{ aspectRatio: '16/9', cursor: ctrlShown ? 'default' : 'none' }}
      onMouseMove={revealControls}
      onMouseLeave={() => { if (playingRef.current) setCtrlVisible(false); }}
    >
      {/* ── React safe boundary ── */}
      <video ref={videoRef} className="w-full h-full object-contain" playsInline autoPlay />

      {/* No separate click overlay: click is handled on the controls overlay itself */}

      {/* ── Buffering spinner ── */}
      {loading && (
        <div className="absolute inset-0 flex items-center justify-center pointer-events-none" style={{ zIndex: 6 }}>
          <div className="w-14 h-14 rounded-full border-[3px] border-white/10 border-t-accent animate-spin" />
        </div>
      )}

      {/* ── Seek flash ── */}
      {flash && (
        <div
          key={flash.key}
          className="absolute inset-0 flex items-center justify-center pointer-events-none"
          style={{ zIndex: 6, animation: 'kf-flash 0.65s ease-out forwards' }}
        >
          <span
            className="text-white text-sm font-bold tracking-wide px-5 py-2.5 rounded-2xl"
            style={{
              background: 'rgba(0,0,0,0.6)',
              backdropFilter: 'blur(10px)',
              border: '1px solid rgba(255,255,255,0.12)',
            }}
          >
            {flash.text}
          </span>
        </div>
      )}

      {/* ── Action flash ── */}
      {actionFlash && (
        <div
          key={actionFlash.key}
          className="absolute inset-0 flex items-center justify-center pointer-events-none"
          style={{ zIndex: 6, animation: 'kf-action-flash 0.85s cubic-bezier(0.175, 0.885, 0.32, 1.275) forwards' }}
        >
          <div className="w-16 h-16 bg-black/50 backdrop-blur-md rounded-full flex items-center justify-center text-white">
            {actionFlash.type === 'play'
              ? <Play size={32} fill="currentColor" strokeWidth={0} className="ml-1" />
              : <Pause size={32} fill="currentColor" strokeWidth={0} />}
          </div>
        </div>
      )}

      {/* ── Controls overlay — always covers full player; click on video area = play/pause ── */}
      <div
        className="absolute inset-0 flex flex-col justify-end"
        style={{
          zIndex: 10,
          opacity: ctrlShown ? 1 : 0,
          transition: 'opacity 0.3s ease',
          // Always keep pointer-events so clicks reach this div even when "hidden"
          // (opacity:0 but pointer-events:auto lets us capture clicks to revealControls)
          pointerEvents: 'auto',
        }}
        onClick={() => {
          // Debounce: if a second click arrives within 220ms, it's a dblclick — skip
          if (clickTimer.current) return;
          clickTimer.current = setTimeout(() => {
            togglePlay();
            clickTimer.current = null;
          }, 220);
        }}
        onDoubleClick={(e) => {
          e.preventDefault();
          clearTimeout(clickTimer.current);
          clickTimer.current = null;
          toggleFS();
        }}
      >
        {/* Gradient scrim — pointer-events:none so it doesn't block the overlay's click handler */}
        <div
          className="absolute inset-0 pointer-events-none"
          style={{
            background:
              'linear-gradient(to top, rgba(0,0,0,0.88) 0%, rgba(0,0,0,0.35) 22%, transparent 55%)',
          }}
        />

        {/* Controls bar — stopPropagation so clicks here don't toggle play */}
        <div
          className="relative px-2 sm:px-3.5 pb-1 sm:pb-3.5 pt-4 sm:pt-6"
          onClick={(e) => e.stopPropagation()}
          onDoubleClick={(e) => e.stopPropagation()}
        >
          {/* ── Progress bar ────────────────────────────────────────────── */}
          <div className="relative w-full mb-1 sm:mb-2.5">
            {/* Time tooltip */}
            {tip && (
              <div
                className="absolute bottom-full mb-2.5 text-white text-[11px] font-semibold px-2 py-0.5 rounded-lg pointer-events-none select-none"
                style={{
                  left: `${tip.pct * 100}%`,
                  transform: 'translateX(-50%)',
                  background: 'rgba(0,0,0,0.85)',
                  backdropFilter: 'blur(6px)',
                  border: '1px solid rgba(255,255,255,0.1)',
                }}
              >
                {fmt(tip.time)}
              </div>
            )}

            {/* Hit area */}
            <div
              ref={progressRef}
              className="group/bar relative w-full flex items-center cursor-pointer py-1.5 sm:py-2"
              onMouseMove={(e) => {
                const rect = progressRef.current.getBoundingClientRect();
                const p = Math.max(0, Math.min(1, (e.clientX - rect.left) / rect.width));
                setTip({ pct: p, time: p * duration });
              }}
              onMouseLeave={() => setTip(null)}
              onMouseDown={(e) => { e.preventDefault(); setDragging(true); seekTo(e.clientX); }}
            >
              {/* Track */}
              <div
                className="relative w-full rounded-full overflow-hidden group-hover/bar:h-[5px] transition-all duration-150"
                style={{ height: '3px', background: 'rgba(255,255,255,0.18)' }}
              >
                {/* Buffered */}
                <div
                  className="absolute inset-y-0 left-0 rounded-full transition-all duration-300"
                  style={{ width: `${bufPct}%`, background: 'rgba(255,255,255,0.32)' }}
                />
                {/* Played */}
                <div
                  className="absolute inset-y-0 left-0 rounded-full"
                  style={{ width: `${pct}%`, background: '#ff2020' }}
                />
              </div>

              {/* Thumb — sibling so it overflows the track */}
              <div
                className="absolute top-1/2 -translate-y-1/2 -translate-x-1/2 w-3.5 h-3.5 rounded-full bg-white pointer-events-none scale-0 group-hover/bar:scale-100 transition-transform duration-150"
                style={{
                  left: `${pct}%`,
                  boxShadow: '0 0 10px rgba(255,30,30,0.7), 0 2px 6px rgba(0,0,0,0.6)',
                }}
              />
            </div>
          </div>

          {/* ── Control row ──────────────────────────────────────────────── */}
          <div className="flex items-center">
            {/* ─── Left cluster ─── */}
            <div className="flex items-center gap-0.5 flex-1">
              {/* Skip back 10s */}
              <button
                className="relative w-7 h-7 sm:w-9 sm:h-9 flex flex-col items-center justify-center rounded-lg text-white/70 hover:text-white hover:bg-white/10 transition-all"
                onClick={() => {
                  const t = Math.max(0, currentTime - 10);
                  if (videoRef.current) videoRef.current.currentTime = t;;
                  setCT(t);
                  doFlash('−10s');
                }}
                title="Back 10s (← / J)"
              >
                <RotateCcw className="w-3.5 h-3.5 sm:w-[15px] sm:h-[15px]" strokeWidth={2.5} />
                <span className="text-[6px] sm:text-[8px] font-black leading-none mt-[1px]">10</span>
              </button>

              {/* Play / Pause */}
              <button
                className="w-7 h-7 sm:w-10 sm:h-10 flex items-center justify-center rounded-full text-white hover:bg-white/10 transition-all mx-0.5"
                onClick={togglePlay}
                title={playing ? 'Pause (Space)' : 'Play (Space)'}
              >
                {playing
                  ? <Pause className="w-4 h-4 sm:w-[22px] sm:h-[22px]" fill="currentColor" strokeWidth={0} />
                  : <Play className="w-4 h-4 sm:w-[22px] sm:h-[22px]" fill="currentColor" strokeWidth={0} />
                }
              </button>

              {/* Skip forward 10s */}
              <button
                className="relative w-7 h-7 sm:w-9 sm:h-9 flex flex-col items-center justify-center rounded-lg text-white/70 hover:text-white hover:bg-white/10 transition-all"
                onClick={() => {
                  const t = Math.min(duration, currentTime + 10);
                  if (videoRef.current) videoRef.current.currentTime = t;;
                  setCT(t);
                  doFlash('+10s');
                }}
                title="Forward 10s (→ / L)"
              >
                <RotateCw className="w-3.5 h-3.5 sm:w-[15px] sm:h-[15px]" strokeWidth={2.5} />
                <span className="text-[6px] sm:text-[8px] font-black leading-none mt-[1px]">10</span>
              </button>

              {/* Volume */}
              <div className="max-[500px]:hidden flex items-center gap-1 sm:gap-1.5 ml-0.5 sm:ml-1.5 group/vol">
                <button
                  className="w-6 h-6 sm:w-8 sm:h-8 flex items-center justify-center rounded-lg text-white/70 hover:text-white hover:bg-white/10 transition-all shrink-0"
                  onClick={toggleMute}
                  title="Mute (M)"
                >
                  <VolumeIcon className="w-4 h-4 sm:w-[18px] sm:h-[18px]" />
                </button>
                {/* Volume slider — expands on group hover or when actively dragging (focused) */}
                <div className="w-0 overflow-hidden transition-all duration-300 group-hover/vol:w-[50px] sm:group-hover/vol:w-[72px] focus-within:w-[50px] sm:focus-within:w-[72px]">
                  <input
                    type="range" min={0} max={1} step={0.02}
                    value={muted ? 0 : volume}
                    onChange={(e) => changeVolume(parseFloat(e.target.value))}
                    className="kf-vol-slider w-[50px] sm:w-[72px] cursor-pointer"
                  />
                </div>
              </div>

              {/* Time display */}
              <span className="max-[370px]:hidden ml-1 sm:ml-2 text-white/55 text-[9px] sm:text-[11px] font-mono whitespace-nowrap select-none">
                {fmt(currentTime)}<span className="text-white/25 mx-1">/</span>{fmt(duration)}
              </span>
            </div>

            {/* ─── Right cluster ─── */}
            <div className="flex items-center gap-0.5 ml-1 sm:ml-2">
              {/* Playback speed */}
              <div className="relative">
                <button
                  className={`flex items-center gap-1 sm:gap-1.5 h-6 sm:h-8 px-1.5 sm:px-2.5 rounded-lg text-[9px] sm:text-xs font-bold transition-all
                    ${showRate ? 'text-white bg-white/12' : 'text-white/60 hover:text-white hover:bg-white/10'}`}
                  onClick={() => { setShowRate(v => !v); setShowQuality(false); }}
                  title="Playback Speed"
                >
                  <Gauge className="w-3 h-3 sm:w-[14px] sm:h-[14px]" />
                  <span>{rate === 1 ? '1×' : `${rate}×`}</span>
                </button>

                {showRate && (
                  <div
                    className="absolute bottom-8 sm:bottom-11 right-0 rounded-xl sm:rounded-2xl border border-white/10 overflow-hidden shadow-2xl z-20 flex flex-col"
                    style={{ background: 'rgba(10,10,10,0.96)', backdropFilter: 'blur(24px)', minWidth: '90px' }}
                  >
                    <p className="px-2.5 sm:px-3.5 pt-2 pb-1 text-[8px] sm:text-[10px] uppercase tracking-[0.15em] text-white/30 font-semibold shrink-0">
                      Speed
                    </p>
                    <div className="overflow-y-auto max-h-[100px] sm:max-h-[200px] custom-scrollbar">
                      {SPEEDS.map(s => (
                        <button
                          key={s}
                          onClick={() => changeRate(s)}
                          className="w-full px-3 sm:px-3.5 py-1.5 sm:py-2 text-left text-xs sm:text-sm flex items-center justify-between transition-colors hover:bg-white/8"
                          style={{ color: rate === s ? '#ff3a3a' : 'rgba(255,255,255,0.75)' }}
                        >
                          <span className={rate === s ? 'font-semibold' : ''}>
                            {s === 1 ? 'Normal' : `${s}×`}
                          </span>
                          {rate === s && (
                            <span className="w-1.5 h-1.5 sm:w-2 sm:h-2 rounded-full bg-accent flex-shrink-0" />
                          )}
                        </button>
                      ))}
                      <div className="h-1 sm:h-2" />
                    </div>
                  </div>
                )}
              </div>

               {/* Quality selector */}
              {qualities.length > 0 && (
                <div className="relative">
                  <button
                    className={`flex items-center gap-1 sm:gap-1.5 h-6 sm:h-8 px-1.5 sm:px-2.5 rounded-lg text-[9px] sm:text-xs font-bold transition-all
                      ${showQuality ? 'text-white bg-white/12' : 'text-white/60 hover:text-white hover:bg-white/10'}`}
                    onClick={() => { setShowQuality(v => !v); setShowRate(false); }}
                    title="Quality"
                  >
                    <Settings className="w-3 h-3 sm:w-[14px] sm:h-[14px]" />
                    <span>{quality === 'auto' ? 'Auto' : `${quality}p`}</span>
                  </button>

                  {showQuality && (
                    <div
                      className="absolute bottom-8 sm:bottom-11 right-0 rounded-xl sm:rounded-2xl border border-white/10 overflow-hidden shadow-2xl z-20 flex flex-col"
                      style={{ background: 'rgba(10,10,10,0.96)', backdropFilter: 'blur(24px)', minWidth: '100px' }}
                    >
                      <p className="px-2.5 sm:px-3.5 pt-2 pb-1 text-[8px] sm:text-[10px] uppercase tracking-[0.15em] text-white/30 font-semibold shrink-0">
                        Quality
                      </p>
                      <div className="overflow-y-auto max-h-[100px] sm:max-h-[200px] custom-scrollbar">
                        {['auto', ...qualities].map(q => (
                          <button
                            key={q}
                            onClick={() => changeQuality(q)}
                            className="w-full px-3 sm:px-3.5 py-1.5 sm:py-2 text-left text-xs sm:text-sm flex items-center justify-between gap-2 transition-colors hover:bg-white/8"
                            style={{ color: quality === q ? '#ff3a3a' : 'rgba(255,255,255,0.75)' }}
                          >
                            <span className={`flex items-center gap-1.5 ${quality === q ? 'font-semibold' : ''}`}>
                              {q === 'auto' ? 'Auto' : `${q}p`}
                              {q !== 'auto' && q >= 720 && (
                                <span
                                  className="text-[7px] sm:text-[9px] font-black uppercase tracking-wide"
                                  style={{ color: q >= 1080 ? 'rgba(167,139,250,0.8)' : 'rgba(96,165,250,0.8)' }}
                                >
                                  {q >= 1080 ? 'FHD' : 'HD'}
                                </span>
                              )}
                            </span>
                            {quality === q && (
                              <span className="w-1.5 h-1.5 sm:w-2 sm:h-2 rounded-full bg-accent flex-shrink-0" />
                            )}
                          </button>
                        ))}
                        <div className="h-1 sm:h-2" />
                      </div>
                    </div>
                  )}
                </div>
              )}

              {/* Fullscreen */}
              <button
                className="w-7 h-7 sm:w-9 sm:h-9 flex items-center justify-center rounded-lg text-white/70 hover:text-white hover:bg-white/10 transition-all ml-0 sm:ml-0.5"
                onClick={toggleFS}
                title={fullscreen ? 'Exit Fullscreen (F)' : 'Fullscreen (F)'}
              >
                {fullscreen ? <Minimize className="w-[14px] h-[14px] sm:w-[18px] sm:h-[18px]" /> : <Maximize className="w-[14px] h-[14px] sm:w-[18px] sm:h-[18px]" />}
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* ── Global styles ── */}
      <style>{`
        /* Custom scrollbar for menus */
        .custom-scrollbar::-webkit-scrollbar { width: 4px; }
        .custom-scrollbar::-webkit-scrollbar-track { background: transparent; }
        .custom-scrollbar::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.2); border-radius: 4px; }
        .custom-scrollbar::-webkit-scrollbar-thumb:hover { background: rgba(255,255,255,0.4); }

        

        /* Seek / pause flash animation */
        @keyframes kf-flash {
          0%   { opacity: 0; transform: scale(0.8);  }
          18%  { opacity: 1; transform: scale(1.06); }
          65%  { opacity: 1; transform: scale(1);    }
          100% { opacity: 0; transform: scale(0.94); }
        }

        /* Action flash animation */
        @keyframes kf-action-flash {
          0%   { opacity: 0; transform: scale(0.6); }
          25%  { opacity: 1; transform: scale(1.1); }
          50%  { opacity: 1; transform: scale(1); }
          75%  { opacity: 1; transform: scale(1); }
          100% { opacity: 0; transform: scale(1.1); }
        }

        /* Styled range input for volume */
        .kf-vol-slider {
          -webkit-appearance: none;
          appearance: none;
          height: 3px;
          border-radius: 99px;
          background: rgba(255,255,255,0.25);
          outline: none;
          display: block;
        }
        .kf-vol-slider::-webkit-slider-thumb {
          -webkit-appearance: none;
          width: 11px;
          height: 11px;
          border-radius: 50%;
          background: #ffffff;
          cursor: pointer;
          box-shadow: 0 1px 4px rgba(0,0,0,0.5);
        }
        .kf-vol-slider::-moz-range-thumb {
          width: 11px;
          height: 11px;
          border-radius: 50%;
          background: #ffffff;
          border: none;
          cursor: pointer;
        }
      `}</style>
    </div>
  );
}
