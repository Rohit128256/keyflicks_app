'use client';
import { useState, useRef, useEffect } from 'react';
import { useAuthStore } from '@/lib/store';
import { UploadCloud, FileVideo, CheckCircle, AlertTriangle } from 'lucide-react';
import { api } from '@/lib/api';
import toast from 'react-hot-toast';
import { useRouter } from 'next/navigation';
import { fetchEventSource } from '@microsoft/fetch-event-source';export default function UploadPage() {
  const { isAuthenticated } = useAuthStore();
  const router = useRouter();
  const fileInputRef = useRef(null);
  const abortControllerRef = useRef(null);

  const [file, setFile] = useState(null);
  const [isDragging, setIsDragging] = useState(false);
  const [uploadState, setUploadState] = useState('idle'); // idle, requesting, uploading, processing, complete, error
  const [progress, setProgress] = useState(0);
  const [statusMessage, setStatusMessage] = useState('');
  const [videoId, setVideoId] = useState(null);

  useEffect(() => {
    if (!isAuthenticated) {
       toast.error("Please login to upload videos");
       router.push('/login');
    }
  }, [isAuthenticated, router]);

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

  const startUpload = async () => {
    if (!file) return;
    setUploadState('requesting');
    setStatusMessage('Requesting secure upload URL...');
    setError('');

    const originalName = file.name;
    const extension = originalName.slice(originalName.lastIndexOf('.'));
    const cleanName = originalName.replace(/\s+/g, '') || 'video' + extension;

    try {
      // 1. Get Presigned URL
      const res = await api.post(`/generate-upload-url/${cleanName}`);
      const { presigned_url, video_id } = res.data;
      setVideoId(video_id);
      
      // 2. Upload to S3 directly tracking progress
      setUploadState('uploading');
      setStatusMessage('Uploading to storage...');
      
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
            setStatusMessage('Upload complete! Video is now processing...');
            startSSETracker(video_id);
         } else {
            handleError(`Upload failed: ${xhr.statusText}`);
         }
      };

      xhr.onerror = () => handleError('Upload failed due to network error.');
      xhr.send(file);
    } catch (err) {
       handleError(err.response?.data?.error || err.message);
    }
  };

  const startSSETracker = async (vid) => {
     const token = useAuthStore.getState().accessToken;
     abortControllerRef.current = new AbortController();
     
     try {
       await fetchEventSource(`/api/stream-status/${vid}`, {
           headers: {
               Authorization: `Bearer ${token}`,
               Accept: 'text/event-stream',
           },
           signal: abortControllerRef.current.signal,
           onmessage(event) {
               const payload = JSON.parse(event.data);
               setStatusMessage(`Processing status: ${payload.data.status}`);
               if (payload.data.status === 'ready') {
                   setUploadState('complete');
                   setStatusMessage('Processing completed successfully!');
                   abortControllerRef.current.abort();
               } else if (payload.data.status === 'failed') {
                   handleError(`Processing error: ${payload.message || 'Unknown error'}`);
                   abortControllerRef.current.abort();
               }
           },
           onerror(err) {
               setStatusMessage('Connection to status server lost. Processing may still continue in background.');
               abortControllerRef.current.abort();
           }
       });
     } catch (err) {
       // Aborted throws an error we can ignore, or handle natively
     }
  };

  const handleError = (msg) => {
     setUploadState('error');
     setStatusMessage(msg);
     toast.error(msg);
  };

  const reset = () => {
     setFile(null);
     setUploadState('idle');
     setProgress(0);
     setStatusMessage('');
  };

  if (!isAuthenticated) return null;

  return (
    <div className="flex flex-col flex-1 w-full max-w-4xl mx-auto py-12">
      <h1 className="text-3xl font-bold mb-8">Upload Video</h1>
      
      {uploadState === 'idle' && (
         <div 
           onDragOver={onDragOver}
           onDragLeave={onDragLeave}
           onDrop={onDrop}
           className={`border-2 border-dashed rounded-2xl p-16 flex flex-col items-center justify-center transition-colors cursor-pointer ${isDragging ? 'border-accent bg-accent/5' : 'border-border bg-surface-1/50 hover:bg-surface-1'}`}
           onClick={() => fileInputRef.current?.click()}
         >
            <UploadCloud size={64} className={`mb-6 ${isDragging ? 'text-accent' : 'text-[#555]'}`} />
            <h3 className="text-2xl font-medium mb-2">Drag & drop your video here</h3>
            <p className="text-[#aaa] mb-6">or click to browse from your device</p>
            <input 
              type="file" 
              ref={fileInputRef} 
              className="hidden" 
              accept="video/*"
              onChange={handleFileChange}
            />
            {file && (
               <div className="bg-surface-2 px-6 py-4 rounded-xl flex items-center gap-4 mt-4 w-full max-w-md border border-border" onClick={e => e.stopPropagation()}>
                  <FileVideo size={24} className="text-accent" />
                  <div className="flex-1 truncate">
                     <p className="font-medium truncate">{file.name}</p>
                     <p className="text-xs text-[#aaa]">{(file.size / (1024*1024)).toFixed(2)} MB</p>
                  </div>
                  <button onClick={startUpload} className="bg-accent hover:bg-accent-hover text-white px-4 py-2 rounded-lg text-sm transition-colors">Start</button>
               </div>
            )}
         </div>
      )}

      {uploadState !== 'idle' && uploadState !== 'complete' && uploadState !== 'error' && (
         <div className="bg-surface-1 border border-border rounded-2xl p-8 flex flex-col items-center justify-center">
            <div className="w-full bg-surface-2 rounded-full h-4 mb-4 overflow-hidden border border-border">
               <div className="bg-accent h-full transition-all duration-300" style={{ width: `${progress}%` }}></div>
            </div>
            <p className="text-lg font-medium">{statusMessage}</p>
            {uploadState === 'uploading' && <p className="text-[#aaa] mt-2">{progress}%</p>}
            {uploadState === 'processing' && <p className="text-[#aaa] mt-2 text-sm animate-pulse">This may take a few minutes...</p>}
         </div>
      )}

      {uploadState === 'complete' && (
         <div className="bg-green-500/10 border border-green-500/30 rounded-2xl p-10 flex flex-col items-center justify-center text-center">
            <CheckCircle size={64} className="text-green-500 mb-6" />
            <h3 className="text-2xl font-medium mb-2 text-white">Video Ready!</h3>
            <p className="text-[#aaa] mb-6">Your video has been successfully uploaded and processed.</p>
            <div className="bg-[#1c1c1c] px-6 py-4 rounded-xl font-mono text-accent text-lg mb-8 select-all border border-border">
               {videoId}
            </div>
            <div className="flex gap-4">
               <button onClick={() => router.push(`/watch/${videoId}`)} className="bg-accent hover:bg-accent-hover text-white px-6 py-3 rounded-full transition-colors">Watch Video</button>
               <button onClick={reset} className="bg-surface-2 hover:bg-surface-1 border border-border text-white px-6 py-3 rounded-full transition-colors">Upload Another</button>
            </div>
         </div>
      )}

      {uploadState === 'error' && (
         <div className="bg-red-500/10 border border-red-500/30 rounded-2xl p-10 flex flex-col items-center justify-center text-center">
            <AlertTriangle size={64} className="text-red-500 mb-6" />
            <h3 className="text-2xl font-medium mb-2 text-white">Upload Failed</h3>
            <p className="text-[#aaa] mb-8">{statusMessage}</p>
            <button onClick={reset} className="bg-surface-2 hover:bg-[rgba(255,255,255,0.1)] border border-border text-white px-6 py-3 rounded-full transition-colors">Try Again</button>
         </div>
      )}
    </div>
  );
}
