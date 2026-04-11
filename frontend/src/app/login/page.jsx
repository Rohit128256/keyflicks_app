'use client';
import { useState } from 'react';
import { api } from '@/lib/api';
import { useAuthStore } from '@/lib/store';
import { useRouter } from 'next/navigation';
import Link from 'next/link';
import toast from 'react-hot-toast';

export default function LoginPage() {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [loading, setLoading] = useState(false);
  const { setAccessToken } = useAuthStore();
  const router = useRouter();

  const handleLogin = async (e) => {
    e.preventDefault();
    setLoading(true);
    try {
      const res = await api.post('/login', { email, password });
      if (res.data.access_token) {
        setAccessToken(res.data.access_token);
        toast.success("Logged in successfully!");
        router.push('/');
      }
    } catch (err) {
      toast.error(err.response?.data?.error || "Login failed");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="flex flex-col items-center justify-center min-h-[75vh] px-4">
      {/* ── Background glow matching home ── */}
      <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[500px] h-[500px] bg-accent/10 rounded-full blur-[100px] -z-10 pointer-events-none"></div>

      <div 
        className="w-full max-w-md border rounded-3xl p-8 md:p-10 relative flex flex-col items-center z-10"
        style={{
          background: 'rgba(255,255,255,0.03)',
          backdropFilter: 'blur(24px)',
          borderColor: 'rgba(255,255,255,0.08)',
          boxShadow: '0 0 0 1px rgba(255,255,255,0.04) inset, 0 25px 50px rgba(0,0,0,0.5)'
        }}
      >
        {/* inner top glow */}
        <div className="absolute -top-px left-1/2 -translate-x-1/2 w-1/2 h-px bg-gradient-to-r from-transparent via-white/30 to-transparent rounded-full" />

        <h1 className="text-3xl font-black mb-2 text-white drop-shadow-md">Welcome Back</h1>
        <p className="text-white/40 mb-8 font-light text-sm">Sign in to your KeyFlicks account</p>
        
        <form onSubmit={handleLogin} className="w-full flex flex-col gap-4">
           <input 
             type="email" 
             placeholder="Email Address" 
             value={email}
             onChange={(e) => setEmail(e.target.value)}
             className="w-full bg-black/40 px-5 py-4 rounded-2xl border border-white/10 text-white text-sm focus:outline-none focus:border-accent/60 focus:ring-1 focus:ring-accent/30 transition-all placeholder:text-white/20 font-mono shadow-inner"
             required
           />
           <input 
             type="password" 
             placeholder="Password" 
             value={password}
             onChange={(e) => setPassword(e.target.value)}
             className="w-full bg-black/40 px-5 py-4 rounded-2xl border border-white/10 text-white text-sm focus:outline-none focus:border-accent/60 focus:ring-1 focus:ring-accent/30 transition-all placeholder:text-white/20 font-mono shadow-inner"
             required
           />
           
           <button 
             type="submit" 
             disabled={loading}
             className="mt-4 w-full flex items-center justify-center gap-2 px-6 py-4 rounded-2xl font-semibold text-sm text-white transition-all hover:-translate-y-0.5 active:translate-y-0 disabled:opacity-50 disabled:hover:translate-y-0"
             style={{ background: 'linear-gradient(135deg, #e60000 0%, #ff3a3a 100%)', boxShadow: '0 4px 20px rgba(255,0,0,0.4)' }}
           >
              {loading ? <span className="animate-spin h-5 w-5 border-2 border-white border-t-transparent rounded-full"></span> : "Authenticate Session →"}
           </button>
        </form>
        
        <div className="mt-8 text-sm text-white/40 font-light">
           Don't have an account? <Link href="/register" className="text-accent hover:text-white hover:underline transition-colors font-medium">Register here</Link>
        </div>
      </div>
    </div>
  );
}
