import "./globals.css";
import Providers from "./Providers";
import Navbar from "@/components/Navbar";

export const metadata = {
  title: "KeyFlicks",
  description: "Secret Video Streaming Platform",
};

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body className="antialiased font-sans">
        <Providers>
          <div className="flex flex-col min-h-screen">
             <Navbar />
             <main className="flex-1 flex flex-col items-center p-4">
                 <div className="w-full max-w-5xl bg-background rounded-xl relative">
                     {children}
                 </div>
             </main>
             <footer className="text-center p-4 text-[#717171] text-sm border-t border-[rgba(255,255,255,0.1)]">
                 <p>KeyFlicks Premium Streaming Service | Powered by Video.js</p>
             </footer>
          </div>
        </Providers>
      </body>
    </html>
  );
}
