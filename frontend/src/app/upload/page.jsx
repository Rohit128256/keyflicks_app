'use client';
import { useState, useRef, useEffect } from 'react';
import { useAuthStore } from '@/lib/store';
import { UploadCloud, FileVideo, CheckCircle, AlertTriangle, Loader2 } from 'lucide-react';
import { api } from '@/lib/api';
import toast from 'react-hot-toast';
import { useRouter } from 'next/navigation';
import { fetchEventSource } from '@microsoft/fetch-event-source';

export default function UploadPage() {
  const { isAuthenticated } = useAuthStore();
  const router = useRouter();
  const fileInputRef = useRef(null);
  const abortControllerRef = useRef(null);
  const initialCheckDone = useRef(false);

  const [file, setFile] = useState(null);
  const [formData, setFormData] = useState({ title: '', description: '' });
  const [isDragging, setIsDragging] = useState(false);
  const [uploadState, setUploadState] = useState('loading'); // loading, idle, requesting, uploading, processing, complete, error
  const [progress, setProgress] = useState(0);
  const [statusMessage, setStatusMessage] = useState('');
  const [videoId, setVideoId] = useState(null); // HIDDEN from user until success

  const getCookie = (name) => {
    if (typeof document === 'undefined') return null;
    const value = `; ${document.cookie}`;
    const parts = value.split(`; ${name}=`);
    if (parts.length === 2) return parts.pop().split(';').shift();
    return null;
  };

  const deleteCookie = (name) => {
    if (typeof document === 'undefined') return;
    document.cookie = `${name}=; expires=Thu, 01 Jan 1970 00:00:00 UTC; path=/;`;
    // Failsafe in case backend path wasn't perfectly aligned
    document.cookie = `${name}=; expires=Thu, 01 Jan 1970 00:00:00 UTC; path=/api/stream-status;`;
  };

  // 1. Initial Mount Check
  useEffect(() => {
    if (!isAuthenticated) return;

    let localAbort = new AbortController();
    abortControllerRef.current = localAbort;

    const checkExistingSession = async () => {
      try {
        const savedId = getCookie('Transcode_status');
        if (!savedId) {
            setUploadState('idle');
            return;
        }

        // IMMEDIATELY jump into the processing UI since we definitively have a tracker cookie
        setUploadState('processing');
        setStatusMessage('Resuming active connection to processing vault...');

        const token = useAuthStore.getState().accessToken;

        await fetchEventSource(`/api/stream-status?video_id=${savedId}`, {
            headers: {
                Authorization: `Bearer ${token}`,
                Accept: 'text/event-stream',
            },
            signal: localAbort.signal,
            async onopen(response) {
                // Return gracefully if explicitly instructed there is no valid content
                if (response.status === 204 || response.status === 400 || response.status === 201) {
                    deleteCookie('Transcode_status');
                    setUploadState('idle'); 
                    return; 
                }
            },
            onmessage(event) {
               try {
                   const payload = JSON.parse(event.data);
                   const currentStatus = payload.data?.status;
                   
                   setStatusMessage(`Processing status: ${currentStatus}...`);
                   
                   if (currentStatus === 'ready') {
                       setVideoId(savedId);
                       // Acknowledge receipt to clear backend Redis stream and remote cookies
                       api.post(`/stream-ack?video_id=${savedId}`).catch(console.error);
                       
                       // Explicit local failsafe cleanup
                       deleteCookie('Transcode_status');
                       
                       setUploadState('complete');
                       setStatusMessage('Processing completed successfully!');
                       localAbort.abort();
                   } else if (currentStatus === 'failed') {
                       deleteCookie('Transcode_status');
                       handleError(`Processing error: Video transcoding failed.`);
                       localAbort.abort();
                   }
               } catch(e) { 
                 console.error("SSE parse error", e); 
               }
            },
            onerror(err) {
               // Ignore abort errors from Strict Mode unmounts
               if (localAbort.signal.aborted) return;
               
               setUploadState(prev => {
                   if (prev === 'loading') return 'idle';
                   if (prev === 'processing') {
                       setStatusMessage('Connection lost. The vault may be processing your video offline.');
                   }
                   return prev;
               });
               throw err; // strictly prevent unlimited retries on simple disconnect
            }
        });
      } catch (err) {
        if (!localAbort.signal.aborted) {
           setUploadState(prev => prev === 'loading' ? 'idle' : prev);
        }
      }
    };

    checkExistingSession();
    
    return () => {
        localAbort.abort();
    };
  }, [isAuthenticated]);

  const onDragOver = (e) => {
    e.preventDefault();
    setIsDragging(true);
  };
  
  const onDragLeave = () => setIsDragging(false);
  
  const onDrop = (e) => {
    e.preventDefault();
    setIsDragging(false);
    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      const droppedFile = e.dataTransfer.files[0];
      if (droppedFile.type.startsWith('video/')) {
        setFile(droppedFile);
      } else {
        toast.error("Please drop a valid video file.");
      }
    }
  };

  const handleFileChange = (e) => {
    if (e.target.files && e.target.files[0]) {
      setFile(e.target.files[0]);
    }
  };

  const startUpload = async (e) => {
    e.preventDefault();
    if (!file) {
      toast.error("Please upload a video file first.");
      return;
    }
    if (!formData.title.trim()) {
      toast.error("Please provide a title constraint.");
      return;
    }

    setUploadState('requesting');
    setStatusMessage('Preparing your upload...');

    const originalName = file.name;
    const extension = originalName.slice(originalName.lastIndexOf('.'));
    const cleanName = originalName.replace(/\s+/g, '') || 'video' + extension;

    try {
      // 1. Post JSON Payload to get Pre-signed URL
      const res = await api.post(`/generate-upload-url`, {
          title: formData.title,
          description: formData.description || "",
          filename: cleanName
      });
      const { presigned_url, video_id } = res.data;
      setVideoId(video_id);
      
      // 2. XMLHttpRequest to track binary upload progress
      setUploadState('uploading');
      setStatusMessage('Uploading video file...');
      
      const xhr = new XMLHttpRequest();
      xhr.open('PUT', presigned_url, true);
      xhr.setRequestHeader('Content-Type', file.type);

      xhr.upload.addEventListener('progress', (e) => {
         if (e.lengthComputable) {
            const percent = Math.round((e.loaded / e.total) * 100);
            setProgress(percent);
         }
      });

      xhr.onload = () => {
         if (xhr.status === 200) {
            setUploadState('processing');
            setStatusMessage('Initial transport complete! Vault is now processing...');
            startSSETracker(video_id); 
         } else {
            handleError(`Transport dropped: ${xhr.statusText}`);
         }
      };

      xhr.onerror = () => handleError('Failed to transport bytes due to network disconnection.');
      xhr.send(file);
    } catch (err) {
       handleError(err.response?.data?.error || err.message);
    }
  };

  const startSSETracker = async (activeVideoId) => {
     const token = useAuthStore.getState().accessToken;
     abortControllerRef.current = new AbortController();
     
     // Fallback to state if directly invoked
     const trackId = activeVideoId || videoId;
     
     try {
       await fetchEventSource(`/api/stream-status?video_id=${trackId}`, {
           headers: {
               Authorization: `Bearer ${token}`,
               Accept: 'text/event-stream',
           },
           signal: abortControllerRef.current.signal,
           onmessage(event) {
               try {
                 const payload = JSON.parse(event.data);
                 const currentStatus = payload.data?.status;
                 
                 setStatusMessage(`Processing status: ${currentStatus}...`);
                 
                 if (currentStatus === 'ready') {
                     setVideoId(trackId);
                     api.post(`/stream-ack?video_id=${trackId}`).catch(console.error);
                     deleteCookie('Transcode_status');

                     setUploadState('complete');
                     setStatusMessage('Processing completed successfully!');
                     if (abortControllerRef.current) abortControllerRef.current.abort();
                 } else if (currentStatus === 'failed') {
                     deleteCookie('Transcode_status');
                     handleError(`Processing error: Central transcoding failed.`);
                     if (abortControllerRef.current) abortControllerRef.current.abort();
                 }
               } catch(e){}
           },
           onerror(err) {
               setStatusMessage('Live stream disconnected... waiting on final server acknowledgment.');
               throw err; 
           }
       });
     } catch (err) {}
  };

  const handleError = (msg) => {
     setUploadState('error');
     setStatusMessage(msg);
     toast.error(msg);
  };

  const reset = () => {
     setFile(null);
     setFormData({ title: '', description: '' });
     setUploadState('idle');
     setProgress(0);
     setStatusMessage('');
     setVideoId(null);
  };

  if (!isAuthenticated) return null;

  return (
    <div className="flex flex-col flex-1 w-full max-w-4xl mx-auto py-12 px-4 relative">
      {/* ── Ambient Background Red Glow ── */}
      <div className="absolute top-1/3 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[600px] h-[600px] bg-accent/5 rounded-full blur-[120px] -z-10 pointer-events-none"></div>

      <div className="w-full flex justify-between items-center mb-8">
         <div>
            <h1 className="text-4xl font-black text-white drop-shadow-md tracking-tight">Upload Video</h1>
            <p className="text-white/40 mt-1">Share your content seamlessly on KeyFlicks</p>
         </div>
      </div>
      
      {uploadState === 'loading' && (
         <div className="flex flex-col items-center justify-center py-24 text-white/50">
             <Loader2 size={32} className="animate-spin mb-4 text-accent" />
             <p className="text-sm font-semibold tracking-widest uppercase">Checking active uploads...</p>
         </div>
      )}

      {uploadState === 'idle' && (
         <form 
           onSubmit={startUpload}
           className="border border-white/10 rounded-3xl p-8 lg:p-10 relative overflow-hidden flex flex-col z-10"
           style={{
             background: 'rgba(255,255,255,0.04)',
             backdropFilter: 'blur(32px)',
             boxShadow: '0 0 0 1px rgba(255,255,255,0.06) inset, 0 20px 50px rgba(0,0,0,0.5)'
           }}
         >
            <div className="absolute -top-px left-1/2 -translate-x-1/2 w-1/3 h-px bg-gradient-to-r from-transparent via-white/20 to-transparent rounded-full" />
            
            <div className="flex flex-col md:flex-row gap-8">
                <div className="flex-1 flex flex-col gap-5 min-w-0">
                   <div>
                     <label className="text-xs font-semibold text-white/40 uppercase tracking-widest pl-2 mb-2 block">Video Title</label>
                     <input 
                       type="text" 
                       value={formData.title} 
                       onChange={e => setFormData({...formData, title: e.target.value})} 
                       placeholder="An interesting title..."
                       className="w-full bg-black/40 px-5 py-4 rounded-2xl border border-white/10 text-white text-sm focus:outline-none focus:border-accent/60 focus:ring-1 focus:ring-accent/30 transition-all shadow-inner placeholder:text-white/20"
                       required
                     />
                   </div>
                   <div className="flex-1">
                     <label className="text-xs font-semibold text-white/40 uppercase tracking-widest pl-2 mb-2 block">Description</label>
                     <textarea 
                       value={formData.description} 
                       onChange={e => setFormData({...formData, description: e.target.value})} 
                       placeholder="A brief overview of the video..."
                       className="w-full h-[120px] bg-black/40 px-5 py-4 rounded-2xl border border-white/10 text-white text-sm focus:outline-none focus:border-accent/60 focus:ring-1 focus:ring-accent/30 transition-all shadow-inner placeholder:text-white/20 resize-none font-light leading-relaxed"
                     />
                   </div>
                </div>

                <div className="flex-1 min-w-0">
                   <label className="text-xs font-semibold text-white/40 uppercase tracking-widest pl-2 mb-2 block">Video File</label>
                   {file ? (
                      <div className="h-[210px] bg-surface-2/40 border border-border rounded-2xl p-6 flex flex-col items-center justify-center text-center shadow-inner relative overflow-hidden">
                         <div className="absolute top-2 right-2">
                             <button type="button" onClick={() => setFile(null)} className="text-white/40 hover:text-[#ff5252] transition-colors p-2 text-xs font-semibold">Change</button>
                         </div>
                         <FileVideo size={48} className="text-accent mb-4" />
                         <p className="font-semibold text-white truncate w-full px-4">{file.name}</p>
                         <p className="text-xs text-white/40 mt-1 font-mono">{(file.size / (1024*1024)).toFixed(2)} MB</p>
                      </div>
                   ) : (
                      <div 
                        onDragOver={onDragOver}
                        onDragLeave={onDragLeave}
                        onDrop={onDrop}
                        className={`h-[210px] border-2 border-dashed rounded-2xl flex flex-col items-center justify-center transition-all cursor-pointer ${isDragging ? 'border-accent bg-accent/5 scale-[1.02]' : 'border-white/10 bg-black/40 hover:bg-black/60'} shadow-inner`}
                        onClick={() => fileInputRef.current?.click()}
                      >
                         <UploadCloud size={40} className={`mb-4 transition-colors ${isDragging ? 'text-accent' : 'text-white/20'}`} />
                         <p className="font-semibold text-sm text-white/80">Drag & Drop Binary</p>
                         <p className="text-xs text-white/30 mt-1">or click to browse local storage</p>
                         <input 
                           type="file" 
                           ref={fileInputRef} 
                           className="hidden" 
                           accept="video/*"
                           onChange={handleFileChange}
                         />
                      </div>
                   )}
                </div>
            </div>

            <div className="mt-8 flex justify-end items-center gap-4 border-t border-white/5 pt-6">
                <button 
                  type="submit" 
                  disabled={!file || !formData.title.trim()}
                  className="flex items-center gap-2 px-8 py-4 rounded-2xl font-bold text-sm text-white transition-all hover:-translate-y-0.5 active:translate-y-0 disabled:opacity-50 disabled:hover:translate-y-0"
                  style={{ background: 'linear-gradient(135deg, #e60000 0%, #ff3a3a 100%)', boxShadow: '0 4px 20px rgba(255,0,0,0.4)' }}
                >
                  <UploadCloud size={16} /> Initiate Upload
                </button>
            </div>
         </form>
      )}

      {uploadState !== 'idle' && uploadState !== 'loading' && uploadState !== 'complete' && uploadState !== 'error' && (
         <div 
           className="border rounded-3xl p-16 flex flex-col items-center justify-center relative z-10"
           style={{
             background: 'rgba(255,255,255,0.02)',
             backdropFilter: 'blur(24px)',
             borderColor: 'rgba(255,255,255,0.08)',
             boxShadow: '0 0 0 1px rgba(255,255,255,0.04) inset, 0 10px 40px rgba(0,0,0,0.3)'
           }}
         >
            <div className="w-full max-w-sm bg-black/40 rounded-full h-3 mb-6 relative overflow-hidden border border-white/10 shadow-inner">
               <div className="absolute top-0 bottom-0 left-0 bg-gradient-to-r from-[#e60000] to-[#ff3a3a] transition-all duration-300" style={{ width: `${progress}%` }}>
                  <div className="absolute inset-0 bg-white/20 w-full h-full animate-[shimmer_1s_infinite] -skew-x-12"></div>
               </div>
            </div>
            
            <h3 className="text-xl font-bold text-white tracking-tight text-center">{statusMessage}</h3>
            
            {uploadState === 'uploading' && (
               <p className="text-accent/80 font-mono mt-3 tracking-widest text-sm bg-accent/10 px-3 py-1 rounded-md">{progress}% Transferred</p>
            )}
            {uploadState === 'processing' && (
               <div className="flex items-center gap-2 mt-4 text-white/40 text-sm font-semibold select-none">
                  <div className="w-2 h-2 rounded-full bg-accent animate-pulse"></div> Generating HLS Streams
               </div>
            )}
         </div>
      )}

      {uploadState === 'complete' && (
         <div 
            className="border border-green-500/20 rounded-3xl p-16 flex flex-col items-center justify-center text-center relative z-10"
            style={{
              background: 'rgba(34, 197, 94, 0.05)',
              backdropFilter: 'blur(24px)',
              boxShadow: '0 0 0 1px rgba(34, 197, 94, 0.08) inset, 0 10px 40px rgba(0,0,0,0.3)'
            }}
         >
            <div className="absolute -top-px left-1/2 -translate-x-1/2 w-1/3 h-px bg-gradient-to-r from-transparent via-green-500/50 to-transparent rounded-full" />
            
            <div className="w-24 h-24 rounded-full bg-green-500/10 flex items-center justify-center mb-6 shadow-[0_0_40px_rgba(34,197,94,0.3)]">
               <CheckCircle size={48} className="text-green-500" />
            </div>
            
            <h3 className="text-3xl font-black mb-2 text-white drop-shadow-md">Vault Delivery Successful</h3>
            <p className="text-white/40 mb-8 max-w-sm leading-relaxed font-light">Your video is officially transcoded, encrypted, and structurally distributed to our CDN network.</p>
            
            <p className="text-xs uppercase tracking-widest font-bold text-white/30 mb-2">Unique Access Key</p>
            <div className="bg-black/60 border border-white/10 px-8 py-5 rounded-2xl font-mono text-green-400 text-lg mb-10 select-all shadow-inner tracking-tight">
               {videoId || "Unknown ID"}
            </div>
            
            <div className="flex gap-4">
               <button onClick={() => router.push(`/watch/${videoId}`)} className="bg-green-600 hover:bg-green-500 text-white px-8 py-3.5 rounded-xl font-bold transition-all shadow-[0_4px_20px_rgba(34,197,94,0.3)] hover:-translate-y-0.5">Stream Output</button>
               <button onClick={reset} className="bg-white/5 hover:bg-white/10 border border-white/10 text-white px-8 py-3.5 rounded-xl font-bold transition-all hover:-translate-y-0.5">Queue New File</button>
            </div>
         </div>
      )}

      {uploadState === 'error' && (
         <div 
            className="border border-red-500/20 rounded-3xl p-16 flex flex-col items-center justify-center text-center relative z-10"
            style={{
              background: 'rgba(239, 68, 68, 0.05)',
              backdropFilter: 'blur(24px)',
              boxShadow: '0 0 0 1px rgba(239, 68, 68, 0.08) inset, 0 10px 40px rgba(0,0,0,0.3)'
            }}
         >
            <div className="absolute -top-px left-1/2 -translate-x-1/2 w-1/3 h-px bg-gradient-to-r from-transparent via-red-500/50 to-transparent rounded-full" />
            
            <AlertTriangle size={64} className="text-[#ff5252] mb-6 drop-shadow-[0_0_20px_rgba(255,82,82,0.4)]" />
            <h3 className="text-3xl font-black mb-3 text-white">Upload Failed</h3>
            <p className="text-[#ff5252]/80 mb-8 bg-red-500/10 px-6 py-4 rounded-xl border border-red-500/20 font-mono text-sm max-w-lg">{statusMessage}</p>
            
            <button onClick={reset} className="bg-white/5 hover:bg-white/10 border border-white/10 text-white px-8 py-3.5 rounded-xl font-bold transition-all hover:-translate-y-0.5 shadow-sm">Restart Protocol</button>
         </div>
      )}
    </div>
  );
}
