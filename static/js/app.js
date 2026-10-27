document.addEventListener('DOMContentLoaded', function() {
    console.log('PIRTDICA loaded');
    
    initTableSorting();
    initTableSearch();
    initNotifications();
    initCookieConsent();
});

function playerHeadshotInitials(name) {
    return String(name || '').trim().split(/\s+/).filter(Boolean)
        .slice(0, 2).map(function(part) { return part.charAt(0); })
        .join('').toUpperCase() || '?';
}

window.renderPlayerIdentity = function(container, playerName, headshotUrl, size) {
    if (!container) return;
    const safeName = String(playerName || '');
    const wrapper = document.createElement('span');
    const allowedSize = ['sm', 'md', 'lg'].includes(size) ? size : 'sm';
    wrapper.className = `player-identity player-identity-${allowedSize}`;

    const avatar = document.createElement('span');
    avatar.className = 'player-avatar';
    avatar.setAttribute('aria-hidden', 'true');

    const fallback = document.createElement('span');
    fallback.className = 'player-headshot-fallback';
    fallback.textContent = playerHeadshotInitials(safeName);
    avatar.appendChild(fallback);

    if (headshotUrl) {
        const image = document.createElement('img');
        image.className = 'player-headshot';
        image.src = headshotUrl;
        image.alt = '';
        image.setAttribute('aria-hidden', 'true');
        image.loading = 'lazy';
        image.decoding = 'async';
        image.onerror = function() { avatar.classList.add('is-fallback'); };
        avatar.appendChild(image);
    } else {
        avatar.classList.add('is-fallback');
    }
    wrapper.appendChild(avatar);

    const label = document.createElement('span');
    label.className = 'player-identity-name';
    label.textContent = safeName;
    wrapper.appendChild(label);
    container.replaceChildren(wrapper);
};

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

// ---- Live league scoreboards (home page) ----
(function(){
    var league = (document.body.getAttribute('data-league') || 'nba').toLowerCase();
    if (league !== 'wnba' && league !== 'nba') return;
    var isNba = league === 'nba';
    var section = document.getElementById(isNba ? 'live-nba-section' : 'live-wnba-section');
    if (!section) return;

    var gamesEl = document.getElementById(isNba ? 'live-nba-games' : 'live-wnba-games');
    var dotEl = document.getElementById(isNba ? 'live-nba-dot' : 'live-wnba-dot');
    var endpoint = isNba ? '/api/live-nba-scores' : '/api/live-wnba-scores';

    function esc(s){ var d = document.createElement('div'); d.textContent = (s == null ? '' : s); return d.innerHTML; }

    function pickBadge(p){
        // Only show the target once the prop has actually hit // never prematurely.
        if (!p || !p.hit) return '';
        var line = (p.line === null || p.line === undefined) ? '' : p.line;
        var label = '\uD83C\uDFAF ' + esc(p.side) + ' ' + esc(line) + ' ' + esc(p.stat);
        return ' <span class="lw-pick lw-pick-win" title="PIRTDICA pick HIT // ' + esc(p.current) + '">' + label + '</span>';
    }

    function statLine(p){
        // Every stat in the FanDuel scoring formula (pts, reb, ast, stl, blk, to)
        // plus 3PM, since we suggest props beyond PRA.
        return '<span class="lw-stat">' + esc(p.pts) + '<i>PTS</i></span>' +
               '<span class="lw-stat">' + esc(p.reb) + '<i>REB</i></span>' +
               '<span class="lw-stat">' + esc(p.ast) + '<i>AST</i></span>' +
               '<span class="lw-stat">' + esc(p.threes) + '<i>3PM</i></span>' +
               '<span class="lw-stat">' + esc(p.stl) + '<i>STL</i></span>' +
               '<span class="lw-stat">' + esc(p.blk) + '<i>BLK</i></span>' +
               '<span class="lw-stat">' + esc(p.to) + '<i>TO</i></span>';
    }

    function playerRow(p){
        var playerName = String(p.name || '');
        var avatar = '<span class="player-avatar' +
            (p.headshot_url ? '' : ' is-fallback') + '" aria-hidden="true">' +
            '<span class="player-headshot-fallback">' +
            esc(playerHeadshotInitials(playerName)) + '</span>';
        if (p.headshot_url) {
            avatar += `<img class="player-headshot" src="${esc(p.headshot_url)}" ` +
                `alt="" aria-hidden="true" loading="lazy" decoding="async" ` +
                `onerror="this.parentElement.classList.add('is-fallback')">`;
        }
        avatar += '</span>';
        var identity = '<span class="player-identity player-identity-sm">' +
            avatar + '<span class="player-identity-name">' + esc(playerName) +
            '</span></span>';
        return '<div class="lw-row' + (p.pick ? ' lw-has-pick' : '') + '">' +
            '<span class="lw-pname">' + identity + pickBadge(p.pick) + '</span>' +
            '<span class="lw-fp">' + esc(p.fp) + '<i>FP</i></span>' +
            '<span class="lw-stats">' + statLine(p) + '</span>' +
            '</div>';
    }

    function teamBlock(side){
        var players = side.players || [];
        var header = '<div class="lw-team-head">' +
            '<img src="' + esc(side.logo) + '" class="lw-logo" alt="">' +
            '<span class="lw-team-name">' + esc(side.abbr) + '</span>' +
            '<span class="lw-team-score">' + esc(side.score) + '</span>' +
            '</div>';
        var body = players.length
            ? players.map(playerRow).join('')
            : '<div class="lw-pre">No box score yet</div>';
        return '<div class="lw-team-block">' + header + body + '</div>';
    }

    function renderGame(g){
        var head = '<div class="lw-head">' +
            '<span class="lw-team"><img src="' + esc(g.away.logo) + '" class="lw-logo" alt="">' + esc(g.away.abbr) + ' <b>' + esc(g.away.score) + '</b></span>' +
            '<span class="lw-status' + (g.state === 'in' ? ' lw-status-live' : '') + '">' + esc(g.status) + '</span>' +
            '<span class="lw-team"><b>' + esc(g.home.score) + '</b> ' + esc(g.home.abbr) + ' <img src="' + esc(g.home.logo) + '" class="lw-logo" alt=""></span>' +
            '</div>';
        var rows = '';
        if (g.state === 'pre') {
            rows = '<div class="lw-pre">Tips off ' + esc(g.status) + '</div>';
        } else {
            rows = teamBlock(g.away) + teamBlock(g.home);
        }
        return '<div class="lw-game">' + head + '<div class="lw-rows">' + rows + '</div></div>';
    }

    function load(){
        fetch(endpoint).then(function(r){ return r.json(); }).then(function(data){
            if (!data || !data.has_games || !(data.games && data.games.length)) {
                section.style.display = 'none';
                return;
            }
            section.style.display = '';
            if (dotEl) dotEl.style.display = data.any_live ? 'inline-block' : 'none';
            gamesEl.innerHTML = data.games.map(renderGame).join('');
        }).catch(function(){ /* keep last rendered view */ });
    }
    load();
    setInterval(load, 30000);
})();

// Horizontal, accessible previews on the public subscription pages.
(function() {
    var carousels = document.querySelectorAll('[data-paywall-carousel]');
    if (!carousels.length) return;

    carousels.forEach(function(root) {
        var track = root.querySelector('[data-carousel-track]');
        var slides = Array.prototype.slice.call(root.querySelectorAll('.paywall-preview-slide'));
        var previous = root.querySelector('[data-carousel-prev]');
        var next = root.querySelector('[data-carousel-next]');
        var meter = root.querySelector('[data-carousel-meter]');
        var progress = root.querySelector('[data-carousel-progress]');
        var status = root.querySelector('[data-carousel-status]');
        if (!track || !slides.length) return;

        function activeIndex() {
            var trackLeft = track.getBoundingClientRect().left + track.clientLeft;
            var nearest = 0;
            var distance = Infinity;
            slides.forEach(function(slide, index) {
                var nextDistance = Math.abs(slide.getBoundingClientRect().left - trackLeft);
                if (nextDistance < distance) {
                    distance = nextDistance;
                    nearest = index;
                }
            });
            return nearest;
        }

        function scrollToIndex(index) {
            var bounded = Math.max(0, Math.min(slides.length - 1, index));
            var trackLeft = track.getBoundingClientRect().left + track.clientLeft;
            var offset = slides[bounded].getBoundingClientRect().left - trackLeft;
            track.scrollTo({
                left: track.scrollLeft + offset,
                behavior: window.matchMedia('(prefers-reduced-motion: reduce)').matches ? 'auto' : 'smooth'
            });
        }

        function updateCarousel() {
            var index = activeIndex();
            var total = slides.length;
            if (progress) progress.style.width = ((index + 1) / total * 100) + '%';
            if (meter) meter.setAttribute('aria-valuenow', String(index + 1));
            if (status) status.textContent = String(index + 1).padStart(2, '0') + ' / ' + String(total).padStart(2, '0');
            if (previous) previous.disabled = index <= 0;
            if (next) next.disabled = index >= total - 1;
        }

        if (previous) previous.addEventListener('click', function() {
            scrollToIndex(activeIndex() - 1);
        });
        if (next) next.addEventListener('click', function() {
            scrollToIndex(activeIndex() + 1);
        });

        track.addEventListener('scroll', function() {
            window.requestAnimationFrame(updateCarousel);
        }, { passive: true });
        track.addEventListener('keydown', function(event) {
            if (event.key === 'ArrowRight') {
                event.preventDefault();
                scrollToIndex(activeIndex() + 1);
            } else if (event.key === 'ArrowLeft') {
                event.preventDefault();
                scrollToIndex(activeIndex() - 1);
            }
        });
        track.addEventListener('wheel', function(event) {
            if (event.shiftKey && Math.abs(event.deltaY) > Math.abs(event.deltaX)) {
                track.scrollLeft += event.deltaY;
                event.preventDefault();
            }
        }, { passive: false });
        window.addEventListener('resize', updateCarousel);

        root.querySelectorAll('[data-dva-mode]').forEach(function(button) {
            button.addEventListener('click', function() {
                var mode = button.getAttribute('data-dva-mode');
                root.querySelectorAll('[data-dva-mode]').forEach(function(sibling) {
                    var active = sibling === button;
                    sibling.classList.toggle('is-active', active);
                    sibling.setAttribute('aria-pressed', String(active));
                });
                root.querySelectorAll('[data-dva-dvs]').forEach(function(cell) {
                    cell.textContent = cell.getAttribute(mode === 'fp' ? 'data-dva-fp' : 'data-dva-dvs');
                });
            });
        });

        var schemeSelect = root.querySelector('[data-scheme-demo]');
        if (schemeSelect) {
            schemeSelect.addEventListener('change', function() {
                var team = schemeSelect.value;
                root.querySelectorAll('.preview-scheme-row > i > b[data-scheme-a]').forEach(function(bar) {
                    var value = bar.getAttribute(team === 'b' ? 'data-scheme-b' : 'data-scheme-a');
                    bar.style.width = value + '%';
                });
            });
        }

        var playerSearch = root.querySelector('[data-preview-player-search]');
        if (playerSearch) {
            playerSearch.addEventListener('input', function() {
                var query = playerSearch.value.trim().toLowerCase();
                var visible = 0;
                root.querySelectorAll('[data-preview-player]').forEach(function(row) {
                    var matches = row.getAttribute('data-preview-player').indexOf(query) !== -1;
                    row.hidden = !matches;
                    if (matches) visible += 1;
                });
                var empty = root.querySelector('[data-preview-player-empty]');
                if (empty) empty.hidden = visible > 0;
            });
        }

        root.classList.add('paywall-preview-ready');
        updateCarousel();
    });
})();
