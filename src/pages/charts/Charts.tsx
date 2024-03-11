import { Container, Grid, Paper, Box } from "@mui/material";
import { Realtime } from "./Realtime";
import ReactApexChart from "react-apexcharts";
import React from "react";
import { AnimateController } from "@/utils/AnimateController";

export function Charts() {
  const paperRef = React.useRef<HTMLDivElement>(null);

  React.useEffect(() => {
    let updateAt = performance.now();
    const animateController = new AnimateController(() => {
      const now = performance.now();

      if (now - updateAt > 999) {
        updateAt = now;

        realtime.getNewSeries(realtime.lastDate, {
          min: 10,
          max: 90,
        });

        ApexCharts.exec("realtime", "updateSeries", [
          {
            data: realtime.data,
          },
        ]);
      }
    });

    animateController.play();

    return () => {
      animateController.abort();
    };
  }, []);

  return (
    <Container>
      <Grid container spacing={6}>
        <Grid item xs={12}>
          <Paper ref={paperRef}>
            <ReactApexChart
              options={{
                chart: {
                  id: "realtime",
                  type: "line",
                  animations: {
                    easing: "linear",
                    dynamicAnimation: {
                      speed: 1000,
                    },
                  },
                  toolbar: {
                    show: false,
                  },
                  zoom: { enabled: false },
                },
                stroke: {
                  curve: "smooth",
                },
                xaxis: {
                  type: "datetime",
                  range: realtime.XAXISRANGE,
                  labels: {
                    // show: false,
                  },
                  tooltip: { enabled: false },
                },
                yaxis: {
                  max: 100,
                  show: false,
                  stepSize: 25,
                  tooltip: { enabled: false },
                },
                tooltip: {
                  enabled: false,
                },
              }}
              series={[
                {
                  data: realtime.data.slice(),
                },
              ]}
              type="line"
              height={350}
            />
          </Paper>
        </Grid>
        <Grid item xs={12} sm={6} md={4} lg={3}>
          <Paper>62131531</Paper>
        </Grid>
        <Grid item xs={12}>
          <Box height={2000}></Box>
        </Grid>
      </Grid>
    </Container>
  );
}

const realtime = new Realtime();
realtime.getDayWiseTimeSeries(new Date().getTime(), 7, {
  min: 10,
  max: 90,
});
console.log(realtime.data, realtime.lastDate);
