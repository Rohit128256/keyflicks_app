'use client';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { useState, useEffect } from 'react';
import { initializeAuth, useAuthStore } from '@/lib/store';
import { api } from '@/lib/api';
import { Toaster } from 'react-hot-toast';
import { usePathname, useRouter } from 'next/navigation';

export default function Providers({ children }) {
  const [queryClient] = useState(() => new QueryClient());
  const [isReady, setIsReady] = useState(false);

  const pathname = usePathname();
  const router = useRouter();
  const { isAuthenticated, setAccessToken } = useAuthStore();

  useEffect(() => {
    initializeAuth(api).finally(() => {
      setIsReady(true);
    });
  }, []);

  // 25 Minute Refresh Loop
  useEffect(() => {
    if (isAuthenticated) {
      const interval = setInterval(async () => {
         try {
           const res = await api.get('/refresh-token');
           if (res.data.access_token) {
              setAccessToken(res.data.access_token);
           }
         } catch (err) {
           console.log('Failed to refresh token automatically', err);
         }
      }, 25 * 60 * 1000); // 25 Minutes
      return () => clearInterval(interval);
    }
  }, [isAuthenticated, setAccessToken]);

  // Route Protection
  useEffect(() => {
    if (isReady && !isAuthenticated) {
      if (pathname.startsWith('/watch') || pathname.startsWith('/upload') || pathname.startsWith('/dashboard')) {
        router.push('/login');
      }
    }
  }, [isReady, isAuthenticated, pathname, router]);

  if (!isReady) {
    return (
      <div className="fixed inset-0 flex flex-col items-center justify-center gap-4" style={{ background: '#0f0f0f' }}>
        <div className="w-10 h-10 rounded-full border-[3px] border-white/10 border-t-accent animate-spin" />
        <span className="text-white/20 text-xs tracking-[0.3em] uppercase font-medium">KeyFlicks</span>
      </div>
    );
  }

  return (
    <QueryClientProvider client={queryClient}>
      <Toaster position="bottom-right" toastOptions={{ style: { background: '#272727', color: '#fff' } }} />
      {children}
    </QueryClientProvider>
  );
}
