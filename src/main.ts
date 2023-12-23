import 'bootstrap-vue/dist/bootstrap-vue.min.css'
import 'bootstrap/dist/css/bootstrap.min.css'
import _ from 'lodash'
import App from '@/App.vue'
import axios from 'axios'
import CustomEvents from 'highcharts-custom-events'
import highchartsDumbbell from 'highcharts/modules/dumbbell'
import HC_more from 'highcharts/highcharts-more'
import Highcharts from 'highcharts'
import HighchartsVue from 'highcharts-vue'
import lodash from 'lodash'
import mitt from 'mitt'
import router from '@/router'
import store from '@/store'
import Vue from 'vue'
import VueLodash from 'vue-lodash'
import {BootstrapVue, BootstrapVueIcons} from 'bootstrap-vue'

// Allow cookies in Access-Control requests
axios.defaults.withCredentials = true
axios.interceptors.response.use(response => response, error => Promise.reject(error))

HC_more(Highcharts)
highchartsDumbbell(Highcharts)
CustomEvents(Highcharts)

Vue.config.productionTip = false
Vue.use(BootstrapVue)
Vue.use(BootstrapVueIcons)
Vue.use(HighchartsVue)
Vue.use(require('vue-moment'))
Vue.use(VueLodash, {lodash})

Vue.prototype.$_ = _
Vue.prototype.$loading = () => store.dispatch('context/loadingStart')
Vue.prototype.$eventHub = mitt()
Vue.prototype.$ready = () => store.dispatch('context/loadingComplete')

const apiBaseUrl = process.env.VUE_APP_API_BASE_URL

axios.get(`${apiBaseUrl}/api/config`).then(response => {
  Vue.prototype.$config = response.data

  axios.get(`${apiBaseUrl}/api/user/profile`).then(response => {
    Vue.prototype.$currentUser = response.data
    new Vue({
      router,
      store,
      render: h => h(App)
    }).$mount('#app')

    store.dispatch('context/init').then(_.noop)
  })
})
