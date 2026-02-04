import { createApp } from "vue";
import { createRouter, createWebHistory } from "vue-router";
import App from "./App.vue";
import "./style.css";

import OperadorasTable from "./components/OperadorasTable.vue";
import OperadoraDetalhes from "./components/OperadoraDetalhes.vue";

const routes = [
  { path: "/", component: OperadorasTable },
  { path: "/operadoras/:cnpj", component: OperadoraDetalhes, props: true },
];

const router = createRouter({
  history: createWebHistory(),
  routes,
});

createApp(App).use(router).mount("#app");
