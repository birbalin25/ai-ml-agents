import { NavLink, Outlet, useLocation } from 'react-router-dom';
import { LayoutDashboard, Ticket, PlusCircle } from 'lucide-react';

const NAV_ITEMS = [
  { to: '/', icon: LayoutDashboard, label: 'Dashboard' },
  { to: '/tickets', icon: Ticket, label: 'Tickets' },
  { to: '/tickets/new', icon: PlusCircle, label: 'New Ticket' },
];

const PAGE_TITLES = {
  '/': 'Dashboard',
  '/tickets': 'Tickets',
  '/tickets/new': 'Create Ticket',
};

export default function Layout() {
  const location = useLocation();
  const title =
    PAGE_TITLES[location.pathname] ||
    (location.pathname.startsWith('/tickets/') ? 'Ticket Detail' : 'Support Portal');

  return (
    <div className="app-layout">
      <aside className="sidebar">
        <div className="sidebar-logo">
          <h1>Support Portal</h1>
          <p>Triage &amp; Tracking</p>
        </div>
        <nav className="sidebar-nav">
          {NAV_ITEMS.map(({ to, icon: Icon, label }) => (
            <NavLink
              key={to}
              to={to}
              end={to === '/'}
              className={({ isActive }) => isActive ? 'active' : ''}
            >
              <Icon />
              {label}
            </NavLink>
          ))}
        </nav>
      </aside>
      <div className="main-content">
        <header className="top-bar">
          <h2>{title}</h2>
        </header>
        <main className="page-content">
          <Outlet />
        </main>
      </div>
    </div>
  );
}
