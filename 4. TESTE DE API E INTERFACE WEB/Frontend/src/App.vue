<template>
  <div>
    <header class="topbar">
      <div class="topbar-inner">
        <div class="brand" @click="$router.push('/')">IntuitiveCare</div>
        <nav class="nav">
          <RouterLink to="/">Operadoras</RouterLink>
        </nav>
      </div>
    </header>

    <main class="container">
      <RouterView />
    </main>
  </div>
</template>
