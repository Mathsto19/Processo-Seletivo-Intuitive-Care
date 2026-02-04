<template>
  <section>
    <h2 class="title">Distribuição de despesas por UF</h2>

    <div class="card chartWrap">
      <div v-if="loading" class="overlay muted">Carregando gráfico...</div>

      <div v-else-if="error" class="overlay error">
        Falha ao carregar estatísticas: {{ error }}
      </div>

      <div v-else-if="isEmpty" class="overlay muted">
        Sem dados para exibir.
      </div>

      <canvas ref="canvasEl"></canvas>
    </div>
  </section>
</template>

<script setup>
import { ref, computed, onMounted, onBeforeUnmount, nextTick } from "vue";
import { getEstatisticas } from "../services/api";

import {
  Chart,
  BarController,
  BarElement,
  CategoryScale,
  LinearScale,
  Tooltip,
  Legend,
  Title,
} from "chart.js";

Chart.register(BarController, BarElement, CategoryScale, LinearScale, Tooltip, Legend, Title);

const canvasEl = ref(null);

const loading = ref(false);
const error = ref("");

const labels = ref([]);
const values = ref([]);

const isEmpty = computed(() => !loading.value && !error.value && labels.value.length === 0);

let chart = null;

function formatBRL(v) {
  return new Intl.NumberFormat("pt-BR", { style: "currency", currency: "BRL" }).format(v || 0);
}

function formatCompact(v) {
  return new Intl.NumberFormat("pt-BR", { notation: "compact", maximumFractionDigits: 1 }).format(v || 0);
}

function destroyChart() {
  if (chart) {
    chart.destroy();
    chart = null;
  }
}

function buildChart() {
  if (!canvasEl.value) return;
  if (!labels.value.length) return;

  const ctx = canvasEl.value.getContext("2d");
  if (!ctx) return;

  destroyChart();

  const h = canvasEl.value?.clientHeight || 320;
  const gradient = ctx.createLinearGradient(0, 0, 0, h);
  gradient.addColorStop(0, "rgba(37, 99, 235, 0.95)"); // topo
  gradient.addColorStop(1, "rgba(37, 99, 235, 0.20)"); // base

  chart = new Chart(ctx, {
    type: "bar",
    data: {
      labels: labels.value,
      datasets: [
        {
          label: "Total de despesas",
          data: values.value,
          borderWidth: 1,
          backgroundColor: gradient,
          borderColor: "rgba(37, 99, 235, 0.95)",
          hoverBackgroundColor: "rgba(37, 99, 235, 0.80)",
          borderRadius: 8,
          maxBarThickness: 56,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      plugins: {
        legend: { display: true },
        title: { display: false },
        tooltip: {
          callbacks: {
            label: (item) => formatBRL(item.raw ?? 0),
          },
        },
      },
      scales: {
        x: {
          ticks: {
            autoSkip: true,
            maxRotation: 0,
          },
          grid: { display: false },
        },
        y: {
          ticks: {
            callback: (v) => formatCompact(v),
          },
        },
      },
    },
  });
}

async function load() {
  loading.value = true;
  error.value = "";

  labels.value = [];
  values.value = [];

  try {
    const stats = await getEstatisticas();

    const porUfRaw = Array.isArray(stats?.por_uf) ? stats.por_uf : [];

    // limpa UF inválida
    const porUf = porUfRaw
      .filter((x) => x && x.uf && String(x.uf).toLowerCase() !== "nan")
      .map((x) => ({
        uf: String(x.uf).trim(),
        total: Number(x.total_despesas || 0),
      }))
      .filter((x) => x.uf.length > 0);

    // ordena desc
    porUf.sort((a, b) => b.total - a.total);

    // opcional: limitar para não ficar poluído
    // pega top 12 e agrega resto em "OUTROS"
    const TOP = 12;
    const top = porUf.slice(0, TOP);
    const rest = porUf.slice(TOP);

    const outrosTotal = rest.reduce((acc, x) => acc + x.total, 0);
    const finalData = outrosTotal > 0 ? [...top, { uf: "OUTROS", total: outrosTotal }] : top;

    labels.value = finalData.map((x) => x.uf);
    values.value = finalData.map((x) => x.total);

    // garante que o canvas já existe e tem tamanho
    await nextTick();
    buildChart();
  } catch (e) {
    error.value = e?.message || String(e);
  } finally {
    loading.value = false;
  }
}

onMounted(load);

onBeforeUnmount(() => {
  destroyChart();
});
</script>

<style scoped>
.title {
  margin: 18px 0 10px;
  font-size: 22px;
}

.card {
  border: 1px solid #e5e7eb;
  border-radius: 14px;
  background: #fff;
  position: relative;
}

.chartWrap {
  height: 320px;
  padding: 12px;
  overflow: hidden;
}

.chartWrap canvas {
  width: 100% !important;
  height: 100% !important;
  display: block;
}

.overlay {
  position: absolute;
  inset: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 14px;
  background: rgba(255, 255, 255, 0.65);
  backdrop-filter: blur(2px);
  z-index: 2;
  text-align: center;
}

.muted {
  color: #6b7280;
  font-weight: 600;
}

.error {
  color: #b00020;
  font-weight: 700;
}
</style>
