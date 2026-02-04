<template>
  <section>
    <div class="toolbar">
      <h1>Operadoras</h1>

      <div class="controls">
        <input
          v-model="q"
          class="input"
          placeholder="Buscar por razão social ou CNPJ"
          @input="onSearchInput"
        />

        <select v-model.number="limit" class="select" @change="goToPage(1)">
          <option :value="10">10</option>
          <option :value="20">20</option>
          <option :value="50">50</option>
        </select>
      </div>
    </div>

    <p v-if="loading" class="muted">Carregando operadoras...</p>
    <p v-else-if="error" class="error">
      Falha ao carregar: {{ error }}
    </p>

    <template v-else>
      <p v-if="rows.length === 0" class="muted">Nenhuma operadora encontrada.</p>

      <div v-else class="tableWrap">
        <table class="table">
          <thead>
            <tr>
              <th>CNPJ</th>
              <th>Razão Social</th>
              <th>UF</th>
              <th>Modalidade</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="op in rows" :key="op.cnpj">
              <td class="mono">{{ op.cnpj }}</td>
              <td>{{ op.razao_social }}</td>
              <td>{{ op.uf }}</td>
              <td>{{ op.modalidade }}</td>
              <td class="right">
                <button class="btn" @click="abrirDetalhes(op.cnpj)">Detalhes</button>
              </td>
            </tr>
          </tbody>
        </table>
      </div>

      <div class="pagination">
        <button class="btn" :disabled="page <= 1" @click="goToPage(page - 1)">Anterior</button>
        <span class="muted">Página {{ page }} de {{ totalPages }} ({{ total }} registros)</span>
        <button class="btn" :disabled="page >= totalPages" @click="goToPage(page + 1)">Próxima</button>
      </div>

      <hr class="sep" />

      <GraficoDespesas />
    </template>
  </section>
</template>

<script setup>
import { ref, onMounted } from "vue";
import { useRouter } from "vue-router";
import { listOperadoras } from "../services/api";
import GraficoDespesas from "./GraficoDespesas.vue";

const router = useRouter();

const rows = ref([]);
const total = ref(0);
const totalPages = ref(1);

const page = ref(1);
const limit = ref(10);
const q = ref("");

const loading = ref(false);
const error = ref("");

let searchTimer = null;

async function load() {
  loading.value = true;
  error.value = "";
  try {
    const res = await listOperadoras({ page: page.value, limit: limit.value, q: q.value });
    rows.value = res.data || [];
    total.value = res.total ?? 0;
    totalPages.value = res.total_pages ?? 1;
  } catch (e) {
    error.value = e?.message || String(e);
  } finally {
    loading.value = false;
  }
}

function goToPage(p) {
  page.value = p;
  load();
}

function onSearchInput() {
  // debounce simples pra não bater na API a cada tecla
  clearTimeout(searchTimer);
  searchTimer = setTimeout(() => {
    page.value = 1;
    load();
  }, 250);
}

function abrirDetalhes(cnpj) {
  router.push(`/operadoras/${cnpj}`);
}

onMounted(load);
</script>

<style scoped>
.toolbar{ display:flex; justify-content:space-between; gap:16px; align-items:flex-end; flex-wrap:wrap; }
.controls{ display:flex; gap:8px; align-items:center; }
.input{ padding:8px 10px; min-width:320px; border:1px solid #dcdcdc; border-radius:8px; }
.select{ padding:8px 10px; border:1px solid #dcdcdc; border-radius:8px; }
.tableWrap{ overflow:auto; border:1px solid #eee; border-radius:10px; }
.table{ width:100%; border-collapse:collapse; }
th, td{ padding:10px 12px; border-bottom:1px solid #f0f0f0; text-align:left; }
.right{ text-align:right; }
.mono{ font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace; }
.btn{ padding:8px 10px; border:1px solid #dcdcdc; background:#fff; border-radius:8px; cursor:pointer; }
.btn:disabled{ opacity:.5; cursor:not-allowed; }
.pagination{ display:flex; justify-content:space-between; align-items:center; margin-top:10px; gap:12px; flex-wrap:wrap; }
.muted{ color:#666; }
.error{ color:#b00020; }
.sep{ margin:18px 0; border:none; border-top:1px solid #eee; }
</style>
