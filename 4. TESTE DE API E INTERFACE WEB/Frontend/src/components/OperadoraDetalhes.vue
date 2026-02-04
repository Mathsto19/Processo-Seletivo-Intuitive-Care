<template>
  <section>
    <div class="top">
      <button class="btn" @click="voltar">← Voltar</button>
      <h1>Detalhes da operadora</h1>
    </div>

    <p class="muted mono">CNPJ: {{ cnpj }}</p>

    <p v-if="loading" class="muted">Carregando...</p>
    <p v-else-if="error" class="error">Falha: {{ error }}</p>

    <template v-else>
      <div class="grid">
        <div class="card">
          <h3>Operadora</h3>
          <p><b>Razão Social:</b> {{ operadora?.razao_social || "-" }}</p>
          <p><b>UF:</b> {{ operadora?.uf || "-" }}</p>
          <p><b>Modalidade:</b> {{ operadora?.modalidade || "-" }}</p>
          <p><b>Registro ANS:</b> {{ operadora?.registro_ans || "-" }}</p>
        </div>

        <div class="card">
          <h3>Histórico de despesas</h3>

          <p v-if="despesas.length === 0" class="muted">Sem despesas registradas.</p>

          <div v-else class="tableWrap">
            <table class="table">
              <thead>
                <tr>
                  <th>Ano</th>
                  <th>Trimestre</th>
                  <th class="right">Valor</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="(d, idx) in despesas" :key="idx">
                  <td>{{ d.ano }}</td>
                  <td>{{ d.trimestre }}</td>
                  <td class="right">{{ moeda(d.valor) }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </template>
  </section>
</template>

<script setup>
import { ref, onMounted } from "vue";
import { useRouter } from "vue-router";
import { getOperadora, getDespesas } from "../services/api";

const props = defineProps({
  cnpj: { type: String, required: true },
});

const router = useRouter();

const loading = ref(false);
const error = ref("");

const operadora = ref(null);
const despesas = ref([]);

function voltar() {
  router.push("/");
}

function moeda(v) {
  const n = Number(v || 0);
  return new Intl.NumberFormat("pt-BR", { style: "currency", currency: "BRL" }).format(n);
}

async function load() {
  loading.value = true;
  error.value = "";
  try {
    operadora.value = await getOperadora(props.cnpj);
    despesas.value = await getDespesas(props.cnpj);
  } catch (e) {
    error.value = e?.message || String(e);
  } finally {
    loading.value = false;
  }
}

onMounted(load);
</script>

<style scoped>
.top{ display:flex; gap:12px; align-items:center; }
.grid{ display:grid; grid-template-columns: 1fr 1.3fr; gap:14px; margin-top:12px; }
@media (max-width: 900px){ .grid{ grid-template-columns:1fr; } }
.card{ border:1px solid #eee; border-radius:10px; padding:12px; }
.tableWrap{ overflow:auto; border:1px solid #eee; border-radius:10px; margin-top:8px; }
.table{ width:100%; border-collapse:collapse; }
th, td{ padding:10px 12px; border-bottom:1px solid #f0f0f0; text-align:left; }
.right{ text-align:right; }
.btn{ padding:8px 10px; border:1px solid #dcdcdc; background:#fff; border-radius:8px; cursor:pointer; }
.muted{ color:#666; }
.error{ color:#b00020; }
.mono{ font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace; }
</style>
