'use client';
import { useState } from 'react';
import { api } from '@/lib/api';
import { useAuthStore } from '@/lib/store';
import { useRouter } from 'next/navigation';
import Link from 'next/link';
import toast from 'react-hot-toast';

export default function RegisterPage() {
  const [formData, setFormData] = useState({
    username: '',
    email: '',
    password: '',
    dob: ''
  });
  const [profilePic, setProfilePic] = useState(null);
  const [loading, setLoading] = useState(false);
  const { setAccessToken } = useAuthStore();
  const router = useRouter();

  const handleChange = (e) => setFormData({ ...formData, [e.target.name]: e.target.value });

  const handleRegister = async (e) => {
    e.preventDefault();
    setLoading(true);
    try {
      const data = new FormData();
      data.append('username', formData.username);
      data.append('email', formData.email);
      data.append('password', formData.password);
      // Format DOB as RFC3339 generic (append T00:00:00Z)
      data.append('dob', formData.dob ? `${formData.dob}T00:00:00Z` : '');
      
      if (profilePic) {
        data.append('profile_pic', profilePic);
      }

      const res = await api.post('/register', data, {
        headers: {
          'Content-Type': 'multipart/form-data'
        }
      });
      
      if (res.data.access_token) {
        setAccessToken(res.data.access_token);
        toast.success("Registered successfully!");
        router.push('/');
      }
    } catch (err) {
       toast.error(err.response?.data?.error || "Registration failed");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="flex flex-col items-center justify-center min-h-[85vh] px-4 py-8">
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

        <h1 className="text-3xl font-black mb-2 text-white drop-shadow-md">Create Account</h1>
        <p className="text-white/40 mb-8 font-light text-sm">Join the KeyFlicks Platform</p>
        
        <form onSubmit={handleRegister} className="w-full flex flex-col gap-4">
           <input 
             type="text" 
             name="username"
             placeholder="Username" 
             value={formData.username}
             onChange={handleChange}
             className="w-full bg-black/40 px-5 py-4 rounded-2xl border border-white/10 text-white text-sm focus:outline-none focus:border-accent/60 focus:ring-1 focus:ring-accent/30 transition-all placeholder:text-white/20 font-mono shadow-inner"
             required
           />
           <input 
             type="email" 
             name="email"
             placeholder="Email Address" 
             value={formData.email}
             onChange={handleChange}
             className="w-full bg-black/40 px-5 py-4 rounded-2xl border border-white/10 text-white text-sm focus:outline-none focus:border-accent/60 focus:ring-1 focus:ring-accent/30 transition-all placeholder:text-white/20 font-mono shadow-inner"
             required
           />
           <input 
             type="password" 
             name="password"
             placeholder="Password" 
             value={formData.password}
             onChange={handleChange}
             className="w-full bg-black/40 px-5 py-4 rounded-2xl border border-white/10 text-white text-sm focus:outline-none focus:border-accent/60 focus:ring-1 focus:ring-accent/30 transition-all placeholder:text-white/20 font-mono shadow-inner"
             required
           />
           <input 
             type="date" 
             name="dob"
             value={formData.dob}
             onChange={handleChange}
             className="w-full bg-black/40 px-5 py-4 rounded-2xl border border-white/10 text-white text-sm focus:outline-none focus:border-accent/60 focus:ring-1 focus:ring-accent/30 transition-all text-white/50 font-mono shadow-inner uppercase"
             required
             style={{ colorScheme: 'dark' }}
           />
           
           <div className="flex flex-col gap-2 mt-2">
              <label className="text-sm font-medium text-white/60 ml-2">Profile Picture (Optional)</label>
              <input 
                type="file" 
                accept="image/*"
                onChange={(e) => setProfilePic(e.target.files[0])}
                className="w-full bg-black/20 p-2 rounded-2xl border border-white/10 text-sm text-white/40 file:mr-4 file:py-2.5 file:px-5 file:rounded-xl file:border-0 file:text-xs file:font-bold file:uppercase file:tracking-wider file:bg-white/10 file:text-white hover:file:bg-white/20 hover:file:cursor-pointer hover:border-white/20 focus:outline-none transition-all cursor-pointer"
              />
           </div>

           <button 
             type="submit" 
             disabled={loading}
             className="mt-6 w-full flex items-center justify-center gap-2 px-6 py-4 rounded-2xl font-semibold text-sm text-white transition-all hover:-translate-y-0.5 active:translate-y-0 disabled:opacity-50 disabled:hover:translate-y-0"
             style={{ background: 'linear-gradient(135deg, #e60000 0%, #ff3a3a 100%)', boxShadow: '0 4px 20px rgba(255,0,0,0.4)' }}
           >
              {loading ? <span className="animate-spin h-5 w-5 border-2 border-white border-t-transparent rounded-full"></span> : "Create Secure Account →"}
           </button>
        </form>
        
        <div className="mt-8 text-sm text-white/40 font-light">
           Already have an account? <Link href="/login" className="text-accent hover:text-white hover:underline transition-colors font-medium">Sign in here</Link>
        </div>
      </div>
    </div>
  );
}
