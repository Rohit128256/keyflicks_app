'use client';
import { useState, useRef, useEffect } from 'react';
import Link from 'next/link';
import { useAuthStore } from '@/lib/store';
import { LogOut, User, Video, UploadCloud, LayoutDashboard } from 'lucide-react';
import { api } from '@/lib/api';
import { useRouter } from 'next/navigation';

export default function Navbar() {
  const { isAuthenticated, logout } = useAuthStore();
  const router = useRouter();
  const [dropdownOpen, setDropdownOpen] = useState(false);
  const dropdownRef = useRef(null);

  useEffect(() => {
    const handleClickOutside = (event) => {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target)) {
        setDropdownOpen(false);
      }
    };
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  const handleLogout = async () => {
    try {
      await api.post('/logout');
    } catch(err) {
      console.log('Logout API failed natively', err);
    }
    document.cookie = "refresh_token=; expires=Thu, 01 Jan 1970 00:00:00 UTC; path=/api/refresh-token;";
    logout();
    setDropdownOpen(false);
    router.push('/login');
  };

  return (
    <header className="w-full text-center p-5 bg-background border-b border-[rgba(255,255,255,0.1)] flex md:flex-row flex-col items-center justify-between z-50">
      <div className="flex flex-col items-start lg:items-center w-full md:w-auto">
        <Link href="/">
          <h1 className="text-3xl font-black text-white tracking-tighter cursor-pointer hover:text-gray-300 transition-colors drop-shadow-md">KeyFlicks</h1>
        </Link>
        <p className="text-[10px] text-accent tracking-[0.2em] uppercase mt-0.5 font-bold">
           Premium Streaming
        </p>
      </div>

      <nav className="flex items-center gap-4 mt-4 md:mt-0 overflow-visible pb-2 md:pb-0 py-2">
        <Link href="/" className="flex items-center gap-2 px-4 py-2 hover:bg-surface-2 rounded-xl transition-all">
          <Video size={18} className="text-accent" /> Watch
        </Link>
        <Link href="/upload" className="flex items-center gap-2 px-4 py-2 hover:bg-surface-2 rounded-xl transition-all">
          <UploadCloud size={18} className="text-accent" /> Upload
        </Link>

        {isAuthenticated ? (
          <div className="relative" ref={dropdownRef}>
            <button 
              onClick={() => setDropdownOpen(!dropdownOpen)}
              className="flex items-center justify-center w-10 h-10 rounded-full bg-surface-2 border border-white/10 hover:border-accent/50 transition-all focus:outline-none focus:ring-2 focus:ring-accent/30"
            >
              <User size={18} className="text-white/80" />
            </button>
            
            {dropdownOpen && (
              <div className="absolute right-0 mt-3 w-48 bg-[#151515] border border-white/10 rounded-2xl shadow-2xl py-2 z-[100] animate-in fade-in slide-in-from-top-2 flex flex-col">
                <div className="px-4 py-2 mb-1 border-b border-white/5">
                  <p className="text-xs text-white/40 uppercase tracking-widest">Account</p>
                </div>
                <Link 
                  href="/profile" 
                  onClick={() => setDropdownOpen(false)}
                  className="flex items-center gap-3 px-4 py-2.5 text-sm text-white/80 hover:text-white hover:bg-white/5 transition-colors"
                >
                  <User size={16} className="text-accent" /> My Profile
                </Link>
                <Link 
                  href="/dashboard" 
                  onClick={() => setDropdownOpen(false)}
                  className="flex items-center gap-3 px-4 py-2.5 text-sm text-white/80 hover:text-white hover:bg-white/5 transition-colors"
                >
                  <LayoutDashboard size={16} className="text-accent" /> Dashboard
                </Link>
                <div className="mx-3 my-1 border-t border-white/5"></div>
                <button 
                  onClick={handleLogout} 
                  className="flex items-center w-full gap-3 px-4 py-2.5 text-sm text-[#ff5252]/80 hover:text-[#ff5252] hover:bg-[#ff5252]/10 transition-colors text-left"
                >
                  <LogOut size={16} /> Logout
                </button>
              </div>
            )}
          </div>
        ) : (
          <Link href="/login" className="flex items-center gap-2 px-5 py-2 bg-accent hover:bg-accent-hover rounded-full text-white font-medium transition-all shadow-[0_4px_14px_0_rgba(255,0,0,0.39)]">
             Login
          </Link>
        )}
      </nav>
    </header>
  );
}
