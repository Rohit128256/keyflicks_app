const fs = require('fs');
let text = fs.readFileSync('c:/Users/rohit/Desktop/latest keyflicks/keyflicks_app/frontend/src/components/VideoPlayer.jsx', 'utf8');

const regex = /\/\/ ── Keyboard shortcuts.*?<div\s+ref=\{containerRef\}/s;

const replacement = `// ── Keyboard shortcuts ────────────────────────────────────────────────────
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
    if (!fullscreen) {
      if (containerRef.current?.requestFullscreen) containerRef.current.requestFullscreen();
      else if (containerRef.current?.webkitRequestFullscreen) containerRef.current.webkitRequestFullscreen();
    } else {
      if (document.fullscreenElement || document.webkitFullscreenElement) {
        if (document.exitFullscreen) document.exitFullscreen();
        else if (document.webkitExitFullscreen) document.webkitExitFullscreen();
      }
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
      ref={containerRef}`;

text = text.replace(regex, replacement);

// Replace remaining stray playerRef commands
text = text.replace(/playerRef\.current\?\.currentTime\(t\)/g, "if (videoRef.current) videoRef.current.currentTime = t;");

fs.writeFileSync('c:/Users/rohit/Desktop/latest keyflicks/keyflicks_app/frontend/src/components/VideoPlayer.jsx', text);
