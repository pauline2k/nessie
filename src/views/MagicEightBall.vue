<template>
  <div>
    <b-row>
      <b-col>
        <h2>🎱 RTL DevOps Project Timeline</h2>
      </b-col>
      <b-col>
        <div>
          <strong v-if="selectedSchedule && !editing">{{ selectedSchedule.name }}</strong>
          <input v-if="selectedSchedule && editing" v-model="selectedSchedule.name" class="w-100" />
        </div>
        <div>
          <span :style="'font-size: 15px; color:' + colors.red">⬤</span> Design
          <span v-if="selectedSchedule && !editing">: <strong>{{ formatDate(selectedSchedule.design) }}</strong></span>
          <input v-if="selectedSchedule && editing" v-model="selectedSchedule.design" />
        </div>
        <div>
          <span :style="'font-size: 15px; color:' + colors.green">⬤</span> Development
          <span v-if="selectedSchedule && !editing">: <strong>{{ formatDate(selectedSchedule.development) }}</strong></span>
          <input v-if="selectedSchedule && editing" v-model="selectedSchedule.development" />
        </div>
        <div>
          <span :style="'font-size: 15px; color:' + colors.blue">⬤</span> QA/bugfix
          <span v-if="selectedSchedule && !editing">: <strong>{{ formatDate(selectedSchedule.qa) }}</strong></span>
          <input v-if="selectedSchedule && editing" v-model="selectedSchedule.qa" />
        </div>
        <div>
          <span :style="'font-size: 15px; color:' + colors.purple">⬤</span> Production release
          <span v-if="selectedSchedule && !editing">: <strong>{{ formatDate(selectedSchedule.release) }}</strong></span>
          <input v-if="selectedSchedule && editing" v-model="selectedSchedule.release" />
        </div>
        <b-btn v-if="selectedSchedule && !editing" @click="editing = true">Edit</b-btn>
        <b-btn v-if="editing" @click="updateSchedule">Save</b-btn>
        <b-btn v-if="editing" @click="cancelUpdateSchedule">Cancel</b-btn>
      </b-col>
    </b-row>
    <div v-if="chartOptions">
      <highcharts :options="chartOptions"></highcharts>
    </div>
  </div>
</template>

<script>
import {Chart} from 'highcharts-vue'
import {get8BallSchedules, update8BallSchedule} from '@/api/magicEightBall'

export default {
  components: {
    highcharts: Chart
  },
  data: () => ({
    chartOptions: null,
    colors: {
      red: '#b22222',
      green: '#33aa33',
      blue: '#6666ff',
      purple: '#bb66bb',
      paleRed: '#ffbbbb',
      paleGreen: '#aaeeaa',
      paleBlue: '#bbccff',
    },
    editing: false,
    schedules: [],
    selectedSchedule: null,
    selectedScheduleIndex: null
  }),
  created() {
    get8BallSchedules().then(schedules => {
      this.schedules = schedules
      this.renderTimeline()
    })
  },
  methods: {
    cancelUpdateSchedule() {
      this.selectedSchedule = this.$_.clone(this.schedules[this.selectedScheduleIndex])
      this.editing = false
    },
    formatDate(datestamp) {
      return new Date(datestamp + 'T00:00-1200').toDateString()
    },
    renderTimeline() {
      let series = {
        design: [],
        development: [],
        qa: []
      }

      let scheduleMin = null
      let scheduleMax = null

      this.$_.each(this.schedules, s => {
        series.design.push({
          name: s.name,
          low: new Date(s.design).getTime(),
          high: new Date(s.development).getTime()
        })
        series.development.push({
          name: s.name,
          low: new Date(s.development).getTime(),
          high: new Date(s.qa).getTime()
        })
        series.qa.push({
          name: s.name,
          low: new Date(s.qa).getTime(),
          high: new Date(s.release).getTime(),
        })
        if (!scheduleMin || scheduleMin > s.design) {
          scheduleMin = s.design
        }
        if (!scheduleMax || scheduleMax < s.release) {
          scheduleMax = s.release
        }
      })

      const colors = this.colors
      const selectSchedule = this.selectSchedule

      this.chartOptions = {
        chart: {
          type: 'dumbbell',
          height: 50 * series.design.length,
          inverted: true,
          zoomType: 'y'
        },
        legend: {
          enabled: false
        },
        tooltip: false,
        xAxis: {
          type: 'category',
          labels: {
            events: {
              click: function() { selectSchedule(this.pos) }
            }
          }
        },
        yAxis: {
          type: 'datetime',
          min: new Date(scheduleMin).getTime(),
          max: new Date(scheduleMax).getTime(),
          title: {
            text: null
          },
          plotLines: [
            {
              color: '#aaa',
              label: {
                rotation: 0,
                style: {
                  color: '#aaa'
                },
                text: new Date().toDateString()
              },
              width: 1,
              zIndex: 9999,
              value: new Date().getTime(),
            }
          ]
        },
        title: {
          text: null
        },
        plotOptions: {
          dumbbell: {
            grouping: false
          }
        },
        series: [
          {
            name: 'Design to development',
            data: series.design,
            connectorWidth: 15,
            color: colors.paleRed,
            lowColor: colors.red,
            marker: {
              fillColor: colors.green,
              symbol: 'circle',
              radius: 7
            }
          },
          {
            name: 'Development to qa',
            data: series.development,
            connectorWidth: 15,
            color: colors.paleGreen,
            lowColor: colors.green,
            marker: {
              fillColor: colors.blue,
              symbol: 'circle',
              radius: 7
            }
          },
          {
            name: 'QA to release',
            data: series.qa,
            connectorWidth: 15,
            lowColor: colors.blue,
            color: colors.paleBlue,
            marker: {
              fillColor: colors.purple,
              symbol: 'circle',
              radius: 7
            }
          }
        ]
      }
      this.$ready()
    },
    selectSchedule(index) {
      this.selectedScheduleIndex = index
      this.selectedSchedule = this.$_.clone(this.schedules[index])
    },
    updateSchedule() {
      update8BallSchedule(this.selectedSchedule.id, this.selectedSchedule).then(updatedSchedule => {
        this.schedules.forEach((schedule, index) => {
          if (schedule.id === updatedSchedule.id) {
            this.schedules.splice(index, 1, updatedSchedule)
            this.selectSchedule(index)
            console.log(this.schedules)
          }
        })
        this.renderTimeline()
        this.editing = false
      })
    }
  }
}
</script>

<style scoped>
h2 {
  font-size: 30px;
}
</style>

<style>
.highcharts-xaxis-labels text {
  cursor: pointer !important;
  font-size: 16px !important;
}

.highcharts-xaxis-labels text:hover {
  font-weight: bold;
}
</style>