document.addEventListener('DOMContentLoaded', function() {
    console.log('PIRTDICA loaded');
    
    initTableSorting();
    initTableSearch();
    initNotifications();
    initCookieConsent();
});

function initTableSorting() {
    document.querySelectorAll('th[data-sortable]').forEach(header => {
        header.style.cursor = 'pointer';
        header.addEventListener('click', function() {
            const table = this.closest('table');
            const tbody = table.querySelector('tbody');
            const rows = Array.from(tbody.querySelectorAll('tr'));
            const colIndex = Array.from(this.parentNode.children).indexOf(this);
            const isNumeric = this.dataset.sortable === 'number';
            
            const currentDir = this.dataset.sortDir || 'none';
            const newDir = currentDir === 'asc' ? 'desc' : 'asc';
            
            table.querySelectorAll('th[data-sortable]').forEach(th => {
                th.dataset.sortDir = 'none';
                th.classList.remove('sort-asc', 'sort-desc');
            });
            
            this.dataset.sortDir = newDir;
            this.classList.add(newDir === 'asc' ? 'sort-asc' : 'sort-desc');
            
            rows.sort((a, b) => {
                let aVal = a.children[colIndex]?.textContent.trim() || '';
                let bVal = b.children[colIndex]?.textContent.trim() || '';
                
                if (isNumeric) {
                    aVal = parseFloat(aVal.replace(/[^0-9.-]/g, '')) || 0;
                    bVal = parseFloat(bVal.replace(/[^0-9.-]/g, '')) || 0;
                    return newDir === 'asc' ? aVal - bVal : bVal - aVal;
                } else {
                    return newDir === 'asc' 
                        ? aVal.localeCompare(bVal)
                        : bVal.localeCompare(aVal);
                }
            });
            
            rows.forEach(row => tbody.appendChild(row));
        });
    });
}

function initTableSearch() {
    document.querySelectorAll('.table-search').forEach(input => {
        input.addEventListener('input', function() {
            const searchTerm = this.value.toLowerCase();
            const tableId = this.dataset.table;
            const table = document.getElementById(tableId);
            if (!table) return;
            
            const rows = table.querySelectorAll('tbody tr');
            rows.forEach(row => {
                const playerCell = row.querySelector('td:first-child');
                const playerName = playerCell?.textContent.toLowerCase() || '';
                row.style.display = playerName.includes(searchTerm) ? '' : 'none';
            });
        });
    });
}

let notifCurrentCategory = 'all';

function initNotifications() {
    const bell = document.getElementById('notifBell');
    if (!bell) return;

    fetchUnreadCount();
    setInterval(fetchUnreadCount, 60000);

    document.addEventListener('click', function(e) {
        const wrapper = document.getElementById('notifWrapper');
        const dropdown = document.getElementById('notifDropdown');
        if (wrapper && dropdown && !wrapper.contains(e.target)) {
            dropdown.style.display = 'none';
        }
    });
}

function fetchUnreadCount() {
    fetch('/api/notifications/unread-count')
        .then(r => r.json())
        .then(data => {
            const badge = document.getElementById('notifBadge');
            if (!badge) return;
            if (data.count > 0) {
                badge.textContent = data.count > 99 ? '99+' : data.count;
                badge.style.display = 'flex';
            } else {
                badge.style.display = 'none';
            }
        })
        .catch(() => {});
}

function toggleNotifications() {
    const dropdown = document.getElementById('notifDropdown');
    if (!dropdown) return;
    const isOpen = dropdown.style.display !== 'none';
    dropdown.style.display = isOpen ? 'none' : 'block';
    if (!isOpen) {
        loadNotifications(notifCurrentCategory);
    }
}

function switchTab(btn) {
    document.querySelectorAll('.notif-tab').forEach(t => t.classList.remove('active'));
    btn.classList.add('active');
    notifCurrentCategory = btn.dataset.category;
    loadNotifications(notifCurrentCategory);
}

function escapeHtml(str) {
    if (!str) return '';
    var d = document.createElement('div');
    d.textContent = str;
    return d.innerHTML;
}

function loadNotifications(category) {
    const list = document.getElementById('notifList');
    if (!list) return;
    list.innerHTML = '<div class="notif-loading">Loading...</div>';

    const url = category && category !== 'all'
        ? '/api/notifications?category=' + category + '&limit=20'
        : '/api/notifications?limit=20';

    fetch(url)
        .then(r => r.json())
        .then(data => {
            if (!data.notifications || data.notifications.length === 0) {
                list.innerHTML = '<div class="notif-empty">No notifications</div>';
                return;
            }
            list.innerHTML = '';
            data.notifications.forEach(n => {
                const item = document.createElement('div');
                item.className = 'notif-item' + (n.is_read ? '' : ' notif-unread');
                item.dataset.id = n.id;
                item.dataset.url = n.action_url || '';
                item.addEventListener('click', function() {
                    clickNotification(parseInt(this.dataset.id), this.dataset.url);
                });

                const catLabels = {
                    competitive: 'Competitive',
                    financial: 'Financial',
                    system: 'System'
                };
                const catClass = n.category ? 'notif-cat notif-cat-' + escapeHtml(n.category) : '';
                const catLabel = catLabels[n.category] || '';

                var topHtml = '<div class="notif-item-top">';
                if (catLabel) topHtml += '<span class="' + catClass + '">' + catLabel + '</span>';
                topHtml += '<span class="notif-time">' + escapeHtml(n.time_ago) + '</span></div>';

                item.innerHTML = topHtml +
                    '<div class="notif-item-title">' + escapeHtml(n.title) + '</div>' +
                    '<div class="notif-item-body">' + escapeHtml(n.body) + '</div>';

                list.appendChild(item);
            });
        })
        .catch(() => {
            list.innerHTML = '<div class="notif-empty">Failed to load</div>';
        });
}

function clickNotification(id, url) {
    fetch('/api/notifications/' + id + '/read', { method: 'POST' })
        .then(() => {
            fetchUnreadCount();
            if (url && url.startsWith('/')) window.location.href = url;
        });
}

function markAllRead() {
    fetch('/api/notifications/read-all', { method: 'POST' })
        .then(() => {
            fetchUnreadCount();
            loadNotifications(notifCurrentCategory);
        });
}

function getCookieValue(name) {
    var v = document.cookie.match('(^|;)\\s*' + name + '\\s*=\\s*([^;]+)');
    return v ? v.pop() : '';
}

function initCookieConsent() {
    var consent = getCookieValue('analytics_consent');
    if (consent === '') {
        var banner = document.getElementById('cookieBanner');
        if (banner) banner.style.display = 'flex';
    } else if (consent === '1') {
        trackPageView();
    }
}

function handleCookieConsent(accepted) {
    fetch('/api/cookie-consent', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ analytics: accepted })
    }).then(function() {
        var banner = document.getElementById('cookieBanner');
        if (banner) banner.style.display = 'none';
        if (accepted) trackPageView();
    }).catch(function() {
        var banner = document.getElementById('cookieBanner');
        if (banner) banner.style.display = 'none';
    });
}

function trackPageView() {
    fetch('/api/track', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            path: window.location.pathname,
            referrer: document.referrer || ''
        })
    }).catch(function() {});
}
